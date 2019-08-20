from __future__ import absolute_import

import pytest
from sqlalchemy import create_engine

import savage
from savage.utils import savage_json_serializer
from tests.db_utils import create_test_db, drop_test_db, get_test_database_url, Session
from tests.models import (
    ArchiveTable,
    Base,
    MultiColumnArchiveTable,
    MultiColumnUserTable,
    UserTable,
)


@pytest.fixture(scope="session")
def engine():
    """Session-wide test database engine."""
    drop_test_db()
    create_test_db()
    savage.init()
    _engine = create_engine(get_test_database_url(), json_serializer=savage_json_serializer)
    Base.metadata.create_all(_engine)
    UserTable.register(ArchiveTable, _engine)
    MultiColumnUserTable.register(MultiColumnArchiveTable, _engine)
    yield _engine
    _engine.dispose()
    drop_test_db()


@pytest.fixture
def session(engine):
    """Creates a new database session for a test."""
    connection = engine.connect()
    _session = Session(bind=engine)

    yield _session

    # Clean up
    _session.close()
    for tablename in Base.metadata.tables.keys():
        connection.execute("DELETE FROM {}".format(tablename))
    connection.close()


@pytest.fixture
def dialect(session):
    return session.bind.dialect


@pytest.fixture
def user_table():
    return UserTable


@pytest.fixture
def p1_dict():
    return dict(product_id=10, col1="foobar", col2=10, col3=True)


@pytest.fixture
def p1(user_table, p1_dict):
    return user_table(**p1_dict)


@pytest.fixture
def p2_dict():
    return dict(product_id=11, col1="baz", col2=11, col3=True)


@pytest.fixture
def p2(user_table, p2_dict):
    return user_table(**p2_dict)


@pytest.fixture
def p3_dict():
    return dict(product_id=2546, col1="test", col2=12, col3=False)


@pytest.fixture
def p3(user_table, p3_dict):
    return user_table(**p3_dict)
