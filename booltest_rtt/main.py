#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import coloredlogs
import logging
import json
import itertools
import shlex
import time
import queue
from jsonpath_ng import jsonpath, parse

from .database import MySQL, Jobs, Experiments, Batteries, Variants, Tests, \
    TestResultEnum, VariantStderr, VariantResults, BatteryErrors, \
    Subtests, Statistics, TestParameters, Pvalues, UserSettings, \
    silent_close, silent_expunge_all, silent_rollback
from .runner import AsyncRunner
from .utils import merge_pvals, booltest_pval


logger = logging.getLogger(__name__)
coloredlogs.install(level=logging.INFO)


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
        self.bool_config = None
        self.parallel_tasks = None
        self.bool_wrapper = None
        self.job_queue = queue.Queue(maxsize=0)
        self.runners = []  # type: List[Optional[AsyncRunner]]
        self.comp_jobs = []  # type: List[Optional[BoolJob]]
        self.results = []

    def init_config(self):
        self.parallel_tasks = self.args.threads or 1
        try:
            with open(self.args.rtt_config) as fh:
                self.rtt_config = json.load(fh)

            self.bool_config = [m.value for m in parse('"toolkit-settings"."booltest"').find(self.rtt_config)][0]
            self.bool_wrapper = [m.value for m in parse('$.wrapper').find(self.bool_config)][0]
            if not self.args.threads:
                self.parallel_tasks = [m.value for m in parse('$."toolkit-settings".execution."max-parallel-tests"').find(self.rtt_config)][0]

        except Exception as e:
            logger.error("Could not load RTT config %s" % (e,), exc_info=e)

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
        dcli = jsonpath('$.default-cli', self.bool_config, True) or ''
        strategies = jsonpath('$.strategies', self.bool_config, False)

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

    def on_finished(self, job, runner, idx):
        if runner.ret_code != 0:
            logger.warning("Return code of job %s is %s" % (idx, runner.ret_code))
            stderr = ("\n".join(runner.err_acc)).strip()
            br = BoolRes(job, runner.ret_code, None, job.is_halving, stderr=stderr)
            self.results.append(br)
            return

        results = runner.out_acc
        buff = (''.join(results)).strip()
        try:
            js = json.loads(buff)
            # print(json.dumps(js, indent=2))

            is_halving = js['halving']
            br = BoolRes(job, 0, js, is_halving)

            if not is_halving:
                br.rejects = [m.value for m in parse('$.inputs[0].res[0].rejects').find(js)][0]
                br.alpha = [m.value for m in parse('$.inputs[0].res[0].ref_alpha').find(js)][0]
                print('rejects: %s, at alpha %.5e' % (br.rejects, br.alpha))

            else:
                br.pval = [m.value for m in parse('$.inputs[0].res[1].halvings[0].pval').find(js)][0]
                print('halving pval:', br.pval)

            self.results.append(br)

        except Exception as e:
            logger.error("Exception processing results: %s" % (e,), exc_info=e)
            print("[[[%s]]]" % buff)

    def on_results_ready(self):
        if self.args.no_db or self.args.eid < 0 or self.args.jid < 0:
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
                alpha = max([x.alpha for x in ok_results])
                pvalue = booltest_pval(nfails=len(rejects), ntests=len(ok_results), alpha=alpha)
                npassed = sum([1 for r in ok_results if not r.rejects])

            bat_db.total_tests = len(ok_results)
            bat_db.passed_tests = npassed
            bat_db.pvalue = pvalue
            bat_db = s.merge(bat_db)

            for rs in self.results:  # type: BoolRes
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
                    cpval = rs.alpha - 1e-20 if rs.rejects else 1
                    st_db = Statistics(name="pvalue", value=cpval, result=passed_res, subtest=sub_db)
                    tp_db = TestParameters(name="alpha", value=rs.alpha, subtest=sub_db)
                    test_db.pvalue = cpval
                    s.add(st_db)
                    s.add(tp_db)

                s.merge(test_db)
            s.commit()

        except Exception as e:
            logger.warning("Exception in storing results: %s" % (e,), exc_info=e)

        finally:
            silent_expunge_all(s)
            silent_close(s)

    def is_halving_battery(self):
        return self.args.battery == 'booltest_2'

    def work(self):
        jobs = [x for x in self.generate_jobs() if x.is_halving() == (self.is_halving_battery())]
        for i, j in enumerate(jobs):
            j.idx = i

        self.runners = [None] * self.parallel_tasks
        self.comp_jobs = [None] * self.parallel_tasks

        for j in jobs:
            self.job_queue.put_nowait(j)

        while not self.job_queue.empty() or sum([1 for x in self.runners if x is not None]) > 0:
            time.sleep(0.1)

            # Realloc work
            for i in range(len(self.runners)):
                if self.runners[i] is not None and self.runners[i].is_running:
                    continue

                was_empty = self.runners[i] is None
                if not was_empty:
                    self.job_queue.task_done()
                    logger.info("Task %d done" % (i,))
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

        self.on_results_ready()

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
        parser.add_argument('--alpha', dest='alpha', type=float, default=1e-4,
                            help='Alpha value for pass/fail')
        parser.add_argument('-t', dest='threads', type=int, default=None,
                            help='Maximum parallel threads')
        return parser


def main():
    br = BoolRunner()
    return br.main()


if __name__ == '__main__':
    main()
