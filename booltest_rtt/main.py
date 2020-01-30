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

from .database import MySQL
from .runner import AsyncRunner


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
    def __init__(self, cli, name, vinfo=''):
        self.cli = cli
        self.name = name
        self.vinfo = vinfo

    def is_halving(self):
        return '--halving' in self.cli


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
        with open(db_cfg["credentials-file"]) as fh:
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

    def on_finished(self, job, results):
        # TODO: process
        buff = (''.join(results)).strip()
        try:
            js = json.loads(buff)
            # print(json.dumps(js, indent=2))

            is_halving = js['halving']
            if not is_halving:
                rejects = [m.value for m in parse('$.inputs[0].res[0].rejects').find(js)][0]
                print('rejects:', rejects)
            else:
                pval = [m.value for m in parse('$.inputs[0].res[1].halvings[0].pval').find(js)][0]
                print('halving pval:', pval)

        except Exception as e:
            logger.error("Exception processing results: %s" % (e,), exc_info=e)
            print("[[[%s]]]" % buff)

    def work(self):
        jobs = self.generate_jobs()
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
                    if self.runners[i].ret_code != 0:
                        logger.warning("Return code of job %s is %s" % (i, self.runners[i].ret_code))

                    else:
                        logger.info("Task %d done" % (i,))
                        self.on_finished(self.comp_jobs[i], self.runners[i].out_acc)

                # Start a new task, if any
                try:
                    job = self.job_queue.get_nowait()  # type: BoolJob
                except queue.Empty:
                    self.runners[i] = None
                    continue

                cli = '%s %s %s' % (self.bool_wrapper, job.cli, self.args.data_path)
                self.comp_jobs[i] = job
                self.runners[i] = get_runner(shlex.split(cli))
                logger.info("Starting async command %s %s, %s" % (job.name, job.vinfo, cli))
                self.runners[i].start()

        # TODO:
        # - load experiment, job ID
        # - load BoolTest config to execute
        # - generate jobs, compute, extract info, add to the database
        pass

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
        parser.add_argument('--rpath',
                            help='Experiment dir')
        parser.add_argument('--no-db', dest='no_db', action='store_const', const=True,
                            help='No database connection')
        parser.add_argument('-t', dest='threads', type=int, default=None,
                            help='Maximum parallel threads')
        return parser


def main():
    br = BoolRunner()
    return br.main()


if __name__ == '__main__':
    main()
