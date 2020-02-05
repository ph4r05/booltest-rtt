#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import enum
import pymysql
from . import errors
from pymysql.err import Warning as MySQLWarning

pymysql.install_as_MySQLdb()

from sqlalchemy import create_engine, UniqueConstraint, ColumnDefault, Index
from sqlalchemy import exc as sa_exc
from sqlalchemy.sql import expression
from sqlalchemy.sql.elements import and_
from sqlalchemy import Column, DateTime, String, Integer, ForeignKey, func, Text, BigInteger, Float, UnicodeText, Enum, TIMESTAMP, FLOAT
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import INTEGER, LONGTEXT, BIGINT
import sqlalchemy as sa
from warnings import filterwarnings


logger = logging.getLogger(__name__)
Base = declarative_base()


class AlembicDataMigration(Base):
    """
    Alembic data migration
    """
    __tablename__ = 'alembic_version_data'
    id = Column(BigInteger, primary_key=True)
    schema_ver = Column(BigInteger)
    data_ver = Column(BigInteger)


class assign(expression.FunctionElement):
    name = 'assign'


class TransientCol(object):
    """Transient column for model projection and comparison."""
    def __init__(self, name, default=None):
        self.name = name
        self.default = default


class DbException(errors.Error):
    """Generic DB exception"""
    def __init__(self, message=None, cause=None):
        super(DbException, self).__init__(message=message, cause=cause)


class MySQL(object):
    def __init__(self, user=None, password=None, db=None, host='127.0.0.1', port=3306, *args, **kwargs):
        self.user = user
        self.password = password
        self.db = db
        self.host = host
        self.port = port

        self.engine = None
        self.session = None

        self.secure_config = None
        self.secure_query = None

    def get_connstring(self):
        con_string = 'mysql://%s:%s@%s%s/%s' % (self.user, self.password,
                                                self.host, ':%s' % self.port,
                                                self.db)
        return con_string

    def build_engine(self, connstring=None, user=None, password=None, store_as_main=True):
        try:
            filterwarnings('ignore', category=MySQLWarning)
            filterwarnings('ignore', category=sa_exc.SAWarning)

            con_str = connstring
            if con_str is None and user is not None:
                con_str = 'mysql://%s:%s@%s%s' % (user, password, self.host, ':%s' % self.port)
            if con_str is None and password is not None:
                con_str = 'mysql://%s:%s@%s%s' % ('root', password, self.host, ':%s' % self.port)
            if con_str is None:
                con_str = self.get_connstring()

            engine = create_engine(con_str, pool_size=256, max_overflow=32, pool_recycle=3600)
            if store_as_main:
                self.engine = engine

            return engine

        except Exception as e:
            logger.info('Exception in building MySQL DB engine %s' % e)
            raise

    def init_db(self):
        self.build_engine()
        self.session = scoped_session(sessionmaker(bind=self.engine))

        # Make sure tables are created
        Base.metadata.create_all(self.engine)

    def get_session(self):
        return self.session()

    def get_engine(self):
        return self.engine

    def execute_sql(self, sql=None, engine=None, ignore_fail=False):
        try:
            if engine is None:
                engine = self.engine

            res = engine.execute(sql)
            return res

        except Exception as e:
            logger.debug('Exception in sql: %s, %s' % (sql, e))
            if not ignore_fail:
                raise

        finally:
            pass

        return None


#
# Helper functions
#


def silent_close(c, quiet=True):
    # noinspection PyBroadException
    try:
        if c is not None:
            c.close()
    except Exception as e:
        if not quiet:
            logger.error('Close exception: %s' % e)


def silent_rollback(c, quiet=True):
    # noinspection PyBroadException
    try:
        if c is not None:
            c.rollback()
    except Exception as e:
        if not quiet:
            logger.error('Rollback exception: %s' % e)


def silent_expunge_all(c, quiet=True):
    # noinspection PyBroadException
    try:
        if c is not None:
            c.expunge_all()()
    except Exception as e:
        if not quiet:
            logger.error('Expunge exception: %s' % e)


#
# DB Entities
#


class StatusEnum(enum.Enum):
    pending = 'pending'
    running = 'running'
    finished = 'finished'
    error = 'error'


class TestResultEnum(enum.Enum):
    passed = 'passed'
    failed = 'failed'


class WorkerTypeEnum(enum.Enum):
    longterm = 'longterm'
    shortterm = 'shortterm'


class Experiments(Base):
    __tablename__ = 'experiments'
    id = Column(BigInteger, primary_key=True)
    name = Column(String(255), nullable=False)
    author_email = Column(String(255), nullable=True)
    status = Column(Enum(StatusEnum), default=StatusEnum.pending)
    created = Column(TIMESTAMP, nullable=False, server_default=func.now())
    run_started = Column(DateTime, nullable=True)
    run_finished = Column(DateTime, nullable=True)
    config_file = Column(String(255), nullable=False)
    data_file = Column(String(255), nullable=False)
    data_file_sha256 = Column(String(64), nullable=False)


class Jobs(Base):
    __tablename__ = 'jobs'
    id = Column(BigInteger, primary_key=True)
    battery = Column(String(100), nullable=False)
    status = Column(Enum(StatusEnum), default=StatusEnum.pending.value)  #, key=True)  # enum key is problem
    run_started = Column(DateTime, nullable=True)
    run_finished = Column(DateTime, nullable=True)
    experiment_id = Column(ForeignKey('experiments.id', name='jobs_experiments_id', ondelete='CASCADE'),
                           nullable=False, index=True, primary_key=False)
    run_heartbeat = Column(DateTime, nullable=True)
    worker_id = Column(BigInteger, nullable=True)
    worker_pid = Column(Integer, nullable=True)
    retries = Column(Integer, nullable=False, default=0)
    lock_version = Column(Integer, nullable=False, default=0)


class Batteries(Base):
    __tablename__ = 'batteries'
    id = Column(BigInteger, primary_key=True)
    name = Column(String(100), nullable=False)
    passed_tests = Column(BigInteger, nullable=False)
    total_tests = Column(BigInteger, nullable=False)
    alpha = Column(Float, nullable=False)
    experiment_id = Column(ForeignKey('experiments.id', name='batteries_experiments_id', ondelete='CASCADE'),
                           nullable=False, index=True, primary_key=False)
    job_id = Column(ForeignKey('jobs.id', name='batteries_jobs__id', ondelete='CASCADE'),
                    nullable=False, index=True, primary_key=False)
    pvalue = Column(Float, nullable=True)
    experiment = relationship('Experiments', foreign_keys=experiment_id)
    job = relationship('Jobs', foreign_keys=job_id)


class BatteryErrors(Base):
    __tablename__ = 'battery_errors'
    id = Column(BigInteger, primary_key=True)
    message = Column(Text, nullable=False)
    battery_id = Column(ForeignKey('batteries.id', name='battery_errors_batteries_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    battery = relationship('Batteries', foreign_keys=battery_id)


class BatteryWarnings(Base):
    __tablename__ = 'battery_warnings'
    id = Column(BigInteger, primary_key=True)
    message = Column(Text, nullable=False)
    battery_id = Column(ForeignKey('batteries.id', name='battery_warnings_batteries_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    battery = relationship('Batteries', foreign_keys=battery_id)


class Tests(Base):
    __tablename__ = 'tests'
    id = Column(BigInteger, primary_key=True)
    name = Column(String(255), nullable=False)
    partial_alpha = Column(Float, nullable=False)
    result = Column(Enum(TestResultEnum), nullable=False)
    test_index = Column(Integer, nullable=False)
    battery_id = Column(ForeignKey('batteries.id', name='tests_batteries_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    pvalue = Column(Float, nullable=True)
    battery = relationship('Batteries', foreign_keys=battery_id)


class Variants(Base):
    __tablename__ = 'variants'
    id = Column(BIGINT(unsigned=True), primary_key=True)
    variant_index = Column(Integer, nullable=False)
    test_id = Column(ForeignKey('tests.id', name='variants_tests_id', ondelete='CASCADE'),
                     nullable=False, index=True, primary_key=False)
    test = relationship('Tests', foreign_keys=test_id)


class VariantsErrors(Base):
    __tablename__ = 'variant_errors'
    id = Column(BigInteger, primary_key=True)
    message = Column(Text, nullable=False)
    variant_id = Column(ForeignKey('variants.id', name='variant_errors_variants_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    variant = relationship('Variants', foreign_keys=variant_id)


class VariantWarnings(Base):
    __tablename__ = 'variant_warnings'
    id = Column(BigInteger, primary_key=True)
    message = Column(Text, nullable=False)
    variant_id = Column(ForeignKey('variants.id', name='variant_warnings_variants_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    variant = relationship('Variants', foreign_keys=variant_id)


class VariantStderr(Base):
    __tablename__ = 'variant_stderr'
    id = Column(BigInteger, primary_key=True)
    message = Column(Text, nullable=False)
    variant_id = Column(ForeignKey('variants.id', name='variant_stderr_variants_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    variant = relationship('Variants', foreign_keys=variant_id)


class VariantResults(Base):
    __tablename__ = 'variant_results'
    id = Column(BigInteger, primary_key=True)
    message = Column(Text, nullable=False)
    variant_id = Column(ForeignKey('variants.id', name='variant_results_variants_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    variant = relationship('Variants', foreign_keys=variant_id)


class UserSettings(Base):
    __tablename__ = 'user_settings'
    id = Column(BigInteger, primary_key=True)
    name = Column(String(100), nullable=False)
    value = Column(String(50), nullable=False)
    variant_id = Column(ForeignKey('variants.id', name='user_settings_variants_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    variant = relationship('Variants', foreign_keys=variant_id)


class RttSettings(Base):
    __tablename__ = 'rtt_settings'
    __table_args__ = (UniqueConstraint('name', name='rtt_settings_name_unique'),)
    id = Column(BigInteger, primary_key=True)
    name = Column(String(100), nullable=False)
    value = Column(String(50), nullable=False)


class Subtests(Base):
    __tablename__ = 'subtests'
    id = Column(BigInteger, primary_key=True)
    subtest_index = Column(Integer, nullable=False)
    variant_id = Column(ForeignKey('variants.id', name='subtests_variants_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    variant = relationship('Variants', foreign_keys=variant_id)


class Statistics(Base):
    __tablename__ = 'statistics'
    id = Column(BigInteger, primary_key=True)
    name = Column(String(255), nullable=False)
    value = Column(Float, nullable=False)
    result = Column(Enum(TestResultEnum), nullable=False)
    subtest_id = Column(ForeignKey('subtests.id', name='statistics_subtests_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    subtest = relationship('Subtests', foreign_keys=subtest_id)


class TestParameters(Base):
    __tablename__ = 'test_parameters'
    id = Column(BigInteger, primary_key=True)
    name = Column(String(100), nullable=False)
    value = Column(String(50), nullable=False)
    subtest_id = Column(ForeignKey('subtests.id', name='test_parameters_subtests_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    subtest = relationship('Subtests', foreign_keys=subtest_id)


class Pvalues(Base):
    __tablename__ = 'p_values'
    id = Column(BigInteger, primary_key=True)
    value = Column(Float, nullable=False)
    subtest_id = Column(ForeignKey('subtests.id', name='p_values_subtests_id', ondelete='CASCADE'),
                        nullable=False, index=True, primary_key=False)
    subtest = relationship('Subtests', foreign_keys=subtest_id)


class Workers(Base):
    __tablename__ = 'workers'
    __table_args__ = (UniqueConstraint('worker_id', name='workers_worker_id_unique'),)
    id = Column(BigInteger, primary_key=True)
    worker_id = Column(String(32), nullable=False)
    worker_name = Column(String(250), nullable=True)
    worker_type = Column(Enum(WorkerTypeEnum), default=WorkerTypeEnum.longterm)
    worker_added = Column(DateTime, nullable=True)
    worker_last_seen = Column(DateTime, nullable=True)
    worker_active = Column(Integer, nullable=False, default=1)
    worker_address = Column(String(250), nullable=True)
    worker_location = Column(String(250), nullable=True)
    worker_aux = Column(Text, nullable=True)






