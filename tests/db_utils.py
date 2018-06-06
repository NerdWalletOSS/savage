import os
from contextlib import contextmanager

from psycopg2 import connect
from sqlalchemy.orm import sessionmaker

PG_CONFIG = dict(user='postgres', password='', host='localhost', port=5433)
CI_PG_CONFIG = dict(PG_CONFIG, port=5432)

MASTER_DATABASE = 'postgres'
TEST_DATABASE = 'savage_test'

Session = sessionmaker()


def get_pg_config():
    if os.environ.get('CI') is not None:
        # Use CI test database
        return CI_PG_CONFIG
    return PG_CONFIG


def get_test_database_url():
    url_template = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    pg_config = get_pg_config()
    return url_template.format(dbname=TEST_DATABASE, **pg_config)


@contextmanager
def _get_master_db_cursor():
    pg_config = get_pg_config()
    master_conn = connect(dbname=MASTER_DATABASE, **pg_config)
    master_conn.autocommit = True  # This is needed to run CREATE/DROP DATABASE statements
    with master_conn.cursor() as cursor:
        yield cursor
    master_conn.close()


def create_test_db():
    # Connect to `postgres` database to drop/create test DB
    with _get_master_db_cursor() as master_cursor:
        master_cursor.execute('CREATE DATABASE "{}"'.format(TEST_DATABASE))


def drop_test_db():
    with _get_master_db_cursor() as master_cursor:
        master_cursor.execute('DROP DATABASE IF EXISTS "{}"'.format(TEST_DATABASE))
