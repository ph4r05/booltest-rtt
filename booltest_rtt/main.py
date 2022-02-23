#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from datetime import datetime

import coloredlogs
import logging
import json
import jsons
import itertools
import shlex
import time
import queue
import sys
import os
import random
import hashlib
from jsonpath_ng import parse
from typing import Optional, List
from sqlalchemy.sql import and_, or_, not_

from .database import MySQL, Jobs, Experiments, Batteries, Variants, Tests, \
    TestResultEnum, VariantStderr, VariantResults, BatteryErrors, \
    Subtests, Statistics, TestParameters, Pvalues, UserSettings, \
    RttBoolResultsCache, silent_close, silent_expunge_all, silent_rollback
from booltest.runner import AsyncRunner
from .utils import merge_pvals, booltest_pval, try_fnc


logger = logging.getLogger(__name__)
coloredlogs.install(level=logging.INFO)


def rand_sleep_time(val=2.0, diff=0.5):
    return max(0.0001, val + random.uniform(0, 2*diff) - diff)


def rand_sleep(val=2.0, diff=0.5):
    time.sleep(rand_sleep_time(val, diff))


def jsonpath(path, obj, allow_none=False):
    r = [m.value for m in parse(path).find(obj)]
    return r[0] if not allow_none else (r[0] if r else None)


def listize(obj):
    return obj if (obj is None or isinstance(obj, list)) else [obj]


def get_runner(cli, cwd=None, rtt_env=None):
    async_runner = AsyncRunner(cli, cwd=cwd, shell=False, env=rtt_env)
    async_runner.log_out_after = False
    async_runner.preexec_setgrp = True
    return async_runner


class BoolParamGen:
    def __init__(self, cli, vals):
        self.cli = cli
        self.vals = vals if isinstance(vals, list) else [vals]


class BoolJob:
    def __init__(self, cli, name, vinfo='', idx=None):
        self.cli = cli
        self.name = name
        self.vinfo = vinfo
        self.idx = idx

    def is_halving(self):
        return '--halving' in self.cli

    def __repr__(self):
        return 'BoolJob(%s, %s, %s)' % (self.name, self.vinfo, self.idx)


class BoolRes:
    def __init__(self, job, ret_code, js_res, is_halving, rejects=False, pval=None, alpha=None, stderr=None):
        self.job = job  # type: BoolJob
        self.ret_code = ret_code
        self.js_res = js_res
        self.is_halving = is_halving
        self.rejects = rejects
        self.alpha = alpha
        self.pval = pval
        self.stderr = stderr


class BoolRunner:
    def __init__(self):
        self.args = None
        self.db = None
        self.rtt_config = None
        self.rtt_config_hash = None
        self.bool_config = None
        self.job_config = None
        self.job_config_hash = None
        self.bool_job_config = None
        self.parallel_tasks = None
        self.bool_wrapper = None
        self.tick_time = 0.15
        self.res_session = None
        self.res_cached = None  # type: Optional[RttBoolResultsCache]
        self.job_queue = queue.Queue(maxsize=0)
        self.runners = []  # type: List[Optional[AsyncRunner]]
        self.comp_jobs = []  # type: List[Optional[BoolJob]]
        self.results = []  # type: List[Optional[BoolRes]]
        self.num_all_jobs = 0

    def init_config(self):
        if not self.args.threads:
            self.parallel_tasks = try_fnc(lambda: int(os.getenv('BOOLTEST_PARALLEL', None)))
        if not self.parallel_tasks:
            self.parallel_tasks = try_fnc(lambda: int(os.getenv('RTT_PARALLEL', None)))

        try:
            with open(self.args.rtt_config) as fh:
                dt = fh.read()
                self.rtt_config = json.loads(dt)
                self.rtt_config_hash = hashlib.sha256(dt.encode("utf8")).hexdigest()

            self.bool_config = jsonpath('"toolkit-settings"."booltest"', self.rtt_config, False)
            if not self.bool_wrapper:
                self.bool_wrapper = jsonpath("$.wrapper", self.bool_config, True)

            if not self.parallel_tasks:
                self.parallel_tasks = jsonpath('$."toolkit-settings".execution."max-parallel-tests-booltest"', self.rtt_config, True)
            if not self.parallel_tasks:
                self.parallel_tasks = jsonpath('$."toolkit-settings".execution."max-parallel-tests"', self.rtt_config, True) or 1

            # TODO: add memory-per-file limit. From file size and BoolTest params we can estimate memory usage and limit
            # number of processes. Works fine if input data is passed as a file (not stdin, not supported now anyway).

        except Exception as e:
            logger.error("Could not load RTT config %s at %s" % (e, self.args.rtt_config), exc_info=e)

        finally:
            if self.parallel_tasks is None:
                self.parallel_tasks = self.args.threads or try_fnc(lambda: int(os.getenv('RTT_PARALLEL', None))) or 1

        if not self.bool_wrapper:
            self.bool_wrapper = "\"%s\" -m booltest.booltest_main" % sys.executable

        self.init_config_job()

    def init_config_job(self):
        if not self.args.config:
            return

        try:
            with open(self.args.config) as fh:
                dt = fh.read()
                self.job_config = json.loads(dt)
                self.job_config_hash = hashlib.sha256(dt.encode("utf8")).hexdigest()

            self.bool_job_config = jsonpath('"randomness-testing-toolkit"."booltest"', self.job_config, True)

        except Exception as e:
            logger.error("Could not load job config %s, file %s" % (e, self.args.config), exc_info=e)

    def init_db(self):
        if self.args.no_db:
            return
        if self.rtt_config is None:
            logger.debug("Could not init DB, no config given")
            return

        db_cfg = [m.value for m in parse('"toolkit-settings"."result-storage"."mysql-db"').find(self.rtt_config)][0]
        db_creds = self.args.db_creds if self.args.db_creds else db_cfg["credentials-file"]
        with open(db_creds) as fh:
            creds = json.load(fh)

        uname = [m.value for m in parse('"credentials"."username"').find(creds)][0]
        passwd = [m.value for m in parse('"credentials"."password"').find(creds)][0]
        host = self.args.db_host if self.args.db_host else db_cfg['address']
        port = self.args.db_port if self.args.db_port else int(db_cfg['port'])

        try:
            self.db = MySQL(user=uname, password=passwd, db=db_cfg['name'], host=host, port=port)
            self.db.init_db()
        except Exception as e:
            logger.warning("Exception in DB connect %s" % (e,), exc_info=e)
            self.db = None

    def generate_jobs(self):
        # Priority: job config default-cli, if not found, take default one from a global RTT config
        dcli_jcfg = jsonpath('$.default-cli', self.bool_job_config, True)
        dcli = dcli_jcfg or jsonpath('$.default-cli', self.bool_config, True) or ''

        # Priority: if bool_job_config contains new strategies, take them, if not, take from a global RTT config
        strategies_jcfg = jsonpath('$.strategies', self.bool_job_config, True)
        strategies = strategies_jcfg or jsonpath('$.strategies', self.bool_config, False)

        # strategies-aux from job config enables to add additional strategies.
        strategies_aux_jcfg = jsonpath('$.strategies-aux', self.bool_job_config, True)
        if strategies_aux_jcfg:
            strategies += strategies_aux_jcfg

        for st in strategies:
            name = st['name']
            st_cli = jsonpath('$.cli', st, True) or ''
            st_vars = jsonpath('$.variations', st, True) or []
            ccli = ('%s %s' % (dcli, st_cli)).strip()

            if not st_vars:
                yield BoolJob(ccli, name)
                continue

            for cvar in st_vars:
                blocks = listize(jsonpath('$.bl', cvar, True)) or [None]
                degs = listize(jsonpath('$.deg', cvar, True)) or [None]
                cdegs = listize(jsonpath('$.cdeg', cvar, True)) or [None]
                pcli = ['--block', '--degree', '--combine-deg']
                vinfo = ['', '', '']
                iterator = itertools.product(blocks, degs, cdegs)

                for el in iterator:
                    c = ' '.join([(('%s %s') % (pcli[ix], dt)) for (ix, dt) in enumerate(el) if dt is not None])
                    vi = '-'.join([(('%s%s') % (vinfo[ix], dt)).strip() for (ix, dt) in enumerate(el) if dt is not None])
                    ccli0 = ('%s %s' % (ccli, c)).strip()

                    yield BoolJob(ccli0, name, vi)

    def run_job(self, cli):
        async_runner = get_runner(shlex.split(cli))

        logger.info("Starting async command %s" % cli)
        async_runner.start()

        while async_runner.is_running:
            time.sleep(1)
        logger.info("Async command finished")

    def on_finished(self, job: BoolJob, runner: Optional[AsyncRunner], idx):
        if runner.ret_code != 0:
            logger.warning("Return code of job %s is %s" % (idx, runner.ret_code))
            stderr = ("\n".join(runner.err_acc)).strip()
            br = BoolRes(job, runner.ret_code, None, job.is_halving, stderr=stderr)
            self.on_add_result(br)
            return

        results = runner.out_acc
        stderr = ("\n".join(runner.err_acc)).strip()
        buff = (''.join(results)).strip()
        try:
            js = json.loads(buff)

            is_halving = js['halving']
            br = BoolRes(job, 0, js, is_halving)

            if not is_halving:
                try:
                    br.rejects = [m.value for m in parse('$.inputs[0].res[0].rejects').find(js)][0]
                    br.alpha = [m.value for m in parse('$.inputs[0].res[0].ref_alpha').find(js)][0]
                    logger.info('rejects: %s, at alpha %.5e' % (br.rejects, br.alpha))
                except Exception as e:
                    logger.warning('BoolTest could not be evaluated, probably missing reference distribution: %s, %s'
                                   % (job, e))
                    br.alpha = 1.0

            else:
                br.pval = [m.value for m in parse('$.inputs[0].res[1].halvings[0].pval').find(js)][0]
                logger.info('halving pval: %5e' % br.pval)

            self.on_add_result(br)

        except Exception as e:
            logger.error("Exception processing results: %s, job: %s, ret code: %s" % (e, job, runner.ret_code),
                         exc_info=e)
            logger.info("Failed BoolTest stderr: %s" % (stderr, ))
            logger.info("[[[%s]]]" % buff)

    def should_use_db(self):
        return not self.args.no_db and self.args.eid >= 0 and self.args.jid >= 0

    def on_add_result(self, br: BoolRes):
        self.results.append(br)
        if not self.should_use_db():
            return

        # Dump all results to db obj
        logger.debug("Storing sub-result to the database")
        if self.res_cached is None:
            self.res_cached = self.init_cached_result_obj()

        self.res_cached.booltest_results = jsons.dumps({'results': self.results})
        self.res_cached.last_update = datetime.now()
        self.res_cached.all_jobs = self.num_all_jobs
        self.res_cached.done_jobs = len(self.results)

        for i in range(10):
            try:
                if self.res_cached.id is None:
                    self.res_session.add(self.res_cached)
                else:
                    self.res_session.merge(self.res_cached)

                self.res_session.flush()
                self.res_session.commit()
                return  # <--- return on success
            except Exception as e:
                logger.warning("Could not update result cache: %s" % (e,), exc_info=e)
                self.res_session.rollback()
                rand_sleep(0.1, 0.05)

        logger.warning("Cached result store failed")

    def on_results_ready(self):
        if not self.should_use_db():
            logger.info("Results will not be inserted to the database. ")
            return

        s = None
        try:
            s = self.db.get_session()
            job_db = s.query(Jobs).filter(Jobs.id == self.args.jid).first()
            exp_db = s.query(Experiments).filter(Experiments.id == self.args.eid).first()

            if not job_db:
                logger.info("Results store fail, could not load Job with id %s" % (self.args.jid,))
                return

            if not exp_db:
                logger.info("Results store fail, could not load Experiment with id %s" % (self.args.eid,))
                return

            bat_db = Batteries(name=self.args.battery, passed_tests=0, total_tests=1, alpha=self.args.alpha,
                               experiment_id=self.args.eid, job_id=self.args.jid)
            s.add(bat_db)
            s.flush()

            bat_errors = ['Job %d (%s-%s), ret_code %d' % (r.job.idx, r.job.name, r.job.vinfo, r.ret_code)
                          for r in self.results if r.ret_code != 0]
            if bat_errors:
                bat_err_db = BatteryErrors(message='\n'.join(bat_errors), battery=bat_db)
                s.add(bat_err_db)

            ok_results = [r for r in self.results if r.ret_code == 0]
            pvalue = -1
            if self.is_halving_battery():
                pvals = [r.pval for r in ok_results]
                npassed = sum([1 for r in ok_results if r.pval >= self.args.alpha])
                pvalue = merge_pvals(pvals)[0] if len(pvals) > 1 else -1

            else:
                rejects = [r for r in ok_results if r.rejects]
                ok_alphas = [x.alpha for x in ok_results if x.alpha is not None]
                alpha = max(ok_alphas) if ok_alphas else self.args.alpha
                pvalue = booltest_pval(nfails=len(rejects), ntests=len(ok_results), alpha=alpha)
                npassed = sum([1 for r in ok_results if not r.rejects])

            bat_db.total_tests = len(ok_results)
            bat_db.passed_tests = npassed
            bat_db.pvalue = float(pvalue) if pvalue is not None else pvalue
            bat_db = s.merge(bat_db)

            for rs in self.results:  # type: BoolRes
                rs.pval = float(rs.pval) if rs.pval is not None else pvalue
                passed = (rs.pval >= self.args.alpha if rs.is_halving else not rs.rejects) if rs.ret_code == 0 else None
                passed_res = (TestResultEnum.passed if passed else TestResultEnum.failed) if passed is not None else TestResultEnum.passed

                test_db = Tests(name="%s %s" % (rs.job.name, rs.job.vinfo), partial_alpha=self.args.alpha,
                                result=passed_res, test_index=rs.job.idx, battery=bat_db)
                s.add(test_db)

                var_db = Variants(variant_index=0, test=test_db)
                s.add(var_db)

                uset_db = UserSettings(name="Cfg", value=rs.job.vinfo, variant=var_db)
                s.add(uset_db)

                if rs.ret_code != 0:
                    var_err_db = VariantStderr(message=rs.stderr, variant=var_db)
                    s.add(var_err_db)
                    continue

                var_res_db = VariantResults(message=json.dumps(rs.js_res), variant=var_db)
                s.add(var_res_db)

                sub_db = Subtests(subtest_index=0, variant=var_db)
                s.add(sub_db)

                if rs.is_halving:
                    st_db = Statistics(name="pvalue", value=rs.pval, result=passed_res, subtest=sub_db)
                    pv_db = Pvalues(value=rs.pval, subtest=sub_db)
                    test_db.pvalue = rs.pval
                    s.add(st_db)
                    s.add(pv_db)

                else:
                    cpval = float(rs.alpha - 1e-20 if rs.rejects else 1)
                    st_db = Statistics(name="pvalue", value=cpval, result=passed_res, subtest=sub_db)
                    tp_db = TestParameters(name="alpha", value=rs.alpha, subtest=sub_db)
                    test_db.pvalue = cpval
                    s.add(st_db)
                    s.add(tp_db)

                s.merge(test_db)
            s.commit()

            try:
                if self.res_cached and self.res_cached.id is not None:
                    s.query(RttBoolResultsCache).filter(RttBoolResultsCache.id == self.res_cached.id).delete()
                    s.commit()
                    logger.info("Deleted cached results with ID %s" % (self.res_cached.id,))
            except Exception as e:
                logger.warning("Unable to remove result cache entry: %s" % (e,), exc_info=e)
                s.rollback()

        except Exception as e:
            logger.warning("Exception in storing results: %s" % (e,), exc_info=e)
            s.rollback()
            raise

        finally:
            silent_expunge_all(s)
            silent_close(s)

    def is_halving_battery(self):
        return self.args.battery == 'booltest_2'

    def get_num_running(self):
        return sum([1 for x in self.runners if x])

    def load_cached_results(self, jobs):
        if not self.should_use_db():
            return

        self.load_cached_results_db()
        if not self.res_cached:
            return jobs

        comp_jobs = set()
        for r in self.results:
            if not r.job:
                continue
            j = r.job
            comp_jobs.add((j.cli, j.name, j.vinfo if j.vinfo else ''))

        njobs = [j for j in jobs if (j.cli, j.name, j.vinfo if j.vinfo else '') not in comp_jobs]
        logger.info("Cached results loaded, all jobs to compute: %s, computed: %s, to compute now: %s"
                    % (len(jobs), len(comp_jobs), len(njobs)))
        return njobs

    def load_cached_results_db(self):
        try:
            self.res_session = s = self.db.get_session()
            res_db = s.query(RttBoolResultsCache).\
                filter(RttBoolResultsCache.job_id == self.args.jid)\
                .order_by(RttBoolResultsCache.last_update.desc())\
                .all()

            if not res_db:
                logger.info("No cached results found for job_id %s" % (self.args.jid,))
                return

            for cres in res_db:  # type: RttBoolResultsCache
                if cres.alpha != self.args.alpha:
                    logger.info("Alpha mismatch: %s vs %s for ID: %s" % (cres.alpha, self.args.alpha, cres.id))
                    continue

                # Simple check, could be done on semantic equivalence of the test configuration
                if cres.rtt_config_hash != self.rtt_config_hash:
                    logger.info("RTT config hash not matching: %s vs %s for ID: %s"
                                % (cres.rtt_config_hash, self.rtt_config_hash, cres.id))
                    continue

                if cres.job_config_hash != self.job_config_hash:
                    logger.info("Job config hash not matching: %s vs %s for ID: %s"
                                % (cres.job_config_hash, self.job_config_hash, cres.id))
                    continue

                self.res_cached = cres
                break

            if not self.res_cached:
                logger.info("No suitable cached result found")
                return

            jsres_obj = json.loads(self.res_cached.booltest_results)
            jsres = jsres_obj['results']
            for ix, jsbres in enumerate(jsres):
                jsjob = try_fnc(lambda: jsbres["job"])
                cjob = None
                if jsjob:
                    cjob = BoolJob(cli=try_fnc(lambda: jsjob['cli']),
                                   name=try_fnc(lambda: jsjob['name']),
                                   vinfo=try_fnc(lambda: jsjob['vinfo']),
                                   idx=try_fnc(lambda: jsjob['idx']))

                bres = BoolRes(job=cjob,
                               ret_code=try_fnc(lambda: jsbres['ret_code']),
                               js_res=try_fnc(lambda: jsbres['js_res']),
                               is_halving=try_fnc(lambda: jsbres['is_halving']),
                               rejects=try_fnc(lambda: jsbres['rejects']),
                               pval=try_fnc(lambda: jsbres['pval']),
                               alpha=try_fnc(lambda: jsbres['alpha']),
                               stderr=try_fnc(lambda: jsbres['stderr']))
                if not bres.js_res:
                    logger.info("Loaded result failed, no js_res for id: %s, rec: %s, job %s"
                                % (self.res_cached.id, ix, cjob))
                    continue

                self.results.append(bres)

        except Exception as e:
            logger.warning("Exception in loading cached results: %s" % (e,), exc_info=e)

    def init_cached_result_obj(self):
        return RttBoolResultsCache(experiment_id=self.args.eid,
                                   job_id=self.args.jid,
                                   job_started=datetime.now(),
                                   last_update=datetime.now(),
                                   alpha=self.args.alpha,
                                   data_path=self.args.data_path,
                                   rtt_config=json.dumps(self.rtt_config),
                                   rtt_config_hash=self.rtt_config_hash,
                                   job_config=json.dumps(self.job_config),
                                   job_config_hash=self.job_config_hash,
                                   all_jobs=0,
                                   done_jobs=0,
                                   )

    def work(self):
        jobs = [x for x in self.generate_jobs() if x.is_halving() == (self.is_halving_battery())]
        for i, j in enumerate(jobs):
            j.idx = i

        self.runners = [None] * self.parallel_tasks  # type: List[Optional[AsyncRunner]]
        self.comp_jobs = [None] * self.parallel_tasks  # type: List[Optional[BoolJob]]
        self.num_all_jobs = len(jobs)

        jobs = self.load_cached_results(jobs)
        if not self.args.no_rand:
            random.shuffle(jobs)

        for j in jobs:
            self.job_queue.put_nowait(j)

        logger.info("Starting BoolTest runner, threads: %s, jobs: %s, wrapper: %s"
                    % (self.parallel_tasks, self.job_queue.qsize(), self.bool_wrapper))

        while not self.job_queue.empty() or sum([1 for x in self.runners if x is not None]) > 0:
            time.sleep(self.tick_time)

            # Realloc work
            for i in range(len(self.runners)):
                if self.runners[i] is not None and self.runners[i].is_running:
                    continue

                was_empty = self.runners[i] is None
                if not was_empty:
                    self.job_queue.task_done()
                    logger.info("Task %d done, job queue size: %d, running: %s"
                                % (i, self.job_queue.qsize(), self.get_num_running()))
                    self.on_finished(self.comp_jobs[i], self.runners[i], i)

                # Start a new task, if any
                try:
                    job = self.job_queue.get_nowait()  # type: BoolJob
                except queue.Empty:
                    self.runners[i] = None
                    continue

                cli = '%s %s "%s"' % (self.bool_wrapper, job.cli, self.args.data_path)
                self.comp_jobs[i] = job
                self.runners[i] = get_runner(shlex.split(cli))
                logger.info("Starting async command %s %s, %s" % (job.name, job.vinfo, cli))
                self.runners[i].start()
                logger.info("Runner %s started, job queue size: %d, running: %s"
                            % (i, self.job_queue.qsize(), self.get_num_running()))

        # attempt to store results several times
        attempts = 30
        for i in range(attempts):
            try:
                self.on_results_ready()
                return
            except Exception as e:
                logger.warning("Could not store results, attempt: %s, err: %s" % (i, e), exc_info=e)
                rand_sleep(2, 0.5)

        # If here, results were not stored
        raise ValueError("Could not store results")

    def main(self):
        logger.debug('App started')

        parser = self.argparser()
        self.args = parser.parse_args()
        self.init_config()
        self.init_db()
        self.work()

    def argparser(self):
        parser = argparse.ArgumentParser(description='BoolTest RTT runner')

        parser.add_argument('--debug', dest='debug', action='store_const', const=True,
                            help='enables debug mode')
        parser.add_argument('-s', '--rtt-config', dest='rtt_config',
                            help='RTT Configuration path')
        parser.add_argument('-b', '--battery', default=None,
                            help='Battery to execute')
        parser.add_argument('-c', '--config', default=None,
                            help='Job config')
        parser.add_argument('-f', '--data-path', dest='data_path', default=None,
                            help='Job data path')
        parser.add_argument('--eid', type=int, default=-1,
                            help='Experiment ID')
        parser.add_argument('--jid', type=int, default=-1,
                            help='Job ID')
        parser.add_argument('--db-host', dest='db_host',
                            help='MySQL host name')
        parser.add_argument('--db-port', dest='db_port', type=int, default=None,
                            help='MySQL port')
        parser.add_argument('--db-creds', dest='db_creds',
                            help='MySQL credentials json')
        parser.add_argument('--rpath',
                            help='Experiment dir')
        parser.add_argument('--no-db', dest='no_db', action='store_const', const=True,
                            help='No database connection')
        parser.add_argument('--no-rand', dest='no_rand', action='store_const', const=True,
                            help='Do not randomize jobs being computed')
        parser.add_argument('--alpha', dest='alpha', type=float, default=1e-5,
                            help='Alpha value for pass/fail')
        parser.add_argument('-t', dest='threads', type=int, default=None,
                            help='Maximum parallel threads')
        return parser


def main():
    br = BoolRunner()
    return br.main()


if __name__ == '__main__':
    main()
