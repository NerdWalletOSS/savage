from __future__ import absolute_import

import sys
import time
from contextlib import contextmanager

import sqlalchemy as sa
from six.moves import range
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.sql.expression import insert

import savage
from savage.models import SavageLogMixin, SavageModelMixin
from tests.db_utils import create_test_db, drop_test_db, get_test_database_url

# --- Setup test database ---
drop_test_db()
create_test_db()


# --- Constants ---
TRIALS = int(sys.argv[1])
VALUE = "TEST"

engine = create_engine(get_test_database_url())
Session = sessionmaker(bind=engine)
Base = declarative_base(bind=engine)


# --- Test Model ---
class TestTable(Base):
    __tablename__ = "test_table"

    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.String(128))


# --- Helpers ---
@contextmanager
def record_time():
    start_time = time.time()
    yield
    end_time = time.time()
    print("execution took {}s".format(end_time - start_time))


def new_environment(Base, c):
    for t in ["test_table", "test_table_archive"]:
        c.execute("drop table if exists {}".format(t))
    Base.metadata.create_all()


# --- Core test ---
print("Running core test...")
conn = engine.connect()
new_environment(Base, conn)
with record_time():
    try:
        for i in range(TRIALS):
            with conn.begin():
                conn.execute(insert(TestTable).values(value=VALUE))
    finally:
        conn.close()


# --- ORM Test ---
print("Running ORM test...")
conn = engine.connect()
new_environment(Base, conn)
session = Session()
with record_time():
    for i in range(TRIALS):
        session.add(TestTable(value=VALUE))
        session.commit()
session.close()
conn.close()

# --- Savage Test ---
SavageBase = declarative_base(bind=engine)


class TestTableArchive(SavageBase, SavageLogMixin):
    __tablename__ = "test_table_archive"
    __table_args__ = (UniqueConstraint("id", "version_id"),)
    id = sa.Column(sa.Integer, primary_key=True)
    user_id = sa.Column(sa.Integer)


class TestTableSavage(SavageBase, SavageModelMixin):
    __tablename__ = "test_table"
    version_columns = ["id"]

    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.String(128))


print("Running Savage test...")
conn = engine.connect()
new_environment(SavageBase, conn)
session = Session()
savage.init()
TestTableSavage.register(TestTableArchive, engine)
with record_time():
    for i in range(TRIALS):
        session.add(TestTableSavage(value=VALUE))
        session.commit()
session.close()
conn.close()


# --- Run bulk Savage test ---
print("Running bulk Savage test...")
conn = engine.connect()
new_environment(SavageBase, conn)
session = Session()

row = {"value": VALUE}
chunk_size = 1000
chunks, remainder = TRIALS / chunk_size, TRIALS % chunk_size
with record_time():
    for _ in range(chunks):
        session.execute(insert(TestTableSavage).values([row] * chunk_size))
    if remainder:
        session.execute(insert(TestTableSavage).values([row] * remainder))
        TestTableArchive.bulk_archive_rows(session.query(TestTableSavage), session)
session.close()
conn.close()


# --- Drop test database ---
engine.dispose()
drop_test_db()
