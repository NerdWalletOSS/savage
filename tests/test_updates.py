import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import StaleDataError

from savage.utils import savage_json_serializer
from tests.db_utils import get_test_database_url, Session
from tests.utils import add_and_return_version, verify_archive, verify_row


@pytest.fixture
def engine_1(engine):
    _engine_1 = create_engine(
        get_test_database_url(),
        isolation_level='READ UNCOMMITTED',
        json_serializer=savage_json_serializer
    )
    yield _engine_1
    _engine_1.dispose()


@pytest.fixture
def engine_2(engine):
    _engine_2 = create_engine(
        get_test_database_url(),
        isolation_level='READ UNCOMMITTED',
        json_serializer=savage_json_serializer
    )
    yield _engine_2
    _engine_2.dispose()


def test_product_update(session, p1_dict, p1):
    version = add_and_return_version(p1, session)

    p1.col1 = 'new'
    p1.col2 = -1
    updated_version = add_and_return_version(p1, session)
    updated_dict = dict(p1_dict, col1='new', col2=-1)

    verify_row(updated_dict, updated_version, session)
    verify_archive(p1_dict, version, session)
    verify_archive(updated_dict, updated_version, session)


def test_product_update_fails(session, user_table, p1):
    """
    Insert a product. Construct a new ORM object with the same id as the inserted object
    and make sure the insertion fails.
    """
    # Initial product insert
    add_and_return_version(p1, session)

    # Create a new row with the same primary key and try to insert it
    p_up_dict = dict(
        col1='newcol',
        col2=5,
        col3=False,
        product_id=10,
    )
    p_up = user_table(**p_up_dict)
    with pytest.raises(IntegrityError):
        add_and_return_version(p_up, session)


def test_update_no_changes(session, user_table, p1_dict, p1):
    '''
    Add an unchanged row and make sure the version does not get bumped.
    '''
    version = add_and_return_version(p1, session)
    p1.col1 = p1_dict['col1']
    session.add(p1)
    session.commit()
    verify_archive(p1_dict, version, session)
    res = session.query(user_table.ArchiveTable).all()
    assert len(res) == 1


def test_multiple_product_updates(session, p1_dict, p1):
    """
    Update a product multiple times and ensure each one gets
    correctly versioned.
    """
    version = add_and_return_version(p1, session)

    p1.col1 = 'new'
    p1.col2 = -1
    version_2 = add_and_return_version(p1, session)
    version_2_dict = dict(p1_dict, col1='new', col2=-1)

    p1.col1 = 'third change'
    p1.col2 = 139
    p1.col3 = False
    version_3 = add_and_return_version(p1, session)
    version_3_dict = dict(p1_dict, col1='third change', col2=139, col3=False)

    verify_row(version_3_dict, version_3, session)
    verify_archive(p1_dict, version, session)
    verify_archive(version_2_dict, version_2, session)
    verify_archive(version_3_dict, version_3, session)


def test_product_update_with_user(session, p1_dict, p1):
    p1.updated_by('test_user1')
    version = add_and_return_version(p1, session)

    p1.col1 = 'new'
    p1.col2 = -1
    p1.updated_by('test_user2')
    updated_version = add_and_return_version(p1, session)
    updated_dict = dict(p1_dict, col1='new', col2=-1)

    verify_row(updated_dict, updated_version, session)
    verify_archive(p1_dict, version, session, user='test_user1')
    verify_archive(updated_dict, updated_version, session, user='test_user2')


def test_concurrent_product_updates(engine_1, engine_2, user_table, p1_dict, p1):
    """
    Assert that if two separate sessions try to update a product row,
    one succeeds and the other fails.
    """
    session_1 = Session(bind=engine_1)
    session_2 = Session(bind=engine_2)
    try:
        # Add the initial row and flush it to the table
        version = add_and_return_version(p1, session_1)

        # Update 1 in session
        p1.col1 = 'changed col 1'
        session_1.add(p1)

        # Update 2 in session 2
        p2 = session_2.query(user_table).first()
        p2.col2 = 1245600

        # this flush should succeed
        version_2 = add_and_return_version(p2, session_2)

        # this flush should fail
        with pytest.raises(StaleDataError):
            session_1.commit()

        final = dict(p1_dict, **dict(col2=1245600))
        verify_row(final, version_2, session_2)
        verify_archive(p1_dict, version, session_2)
        verify_archive(final, version_2, session_2)
    finally:
        # Clean up
        session_1.close()
        session_2.close()
        for t in (user_table, user_table.ArchiveTable):
            engine_1.execute('delete from {}'.format(t.__tablename__))
