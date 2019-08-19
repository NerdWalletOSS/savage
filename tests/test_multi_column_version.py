from __future__ import absolute_import

import pytest
from six.moves import zip
from sqlalchemy.exc import IntegrityError

from savage.api import delete
from tests.models import MultiColumnUserTable
from tests.utils import (
    add_and_return_version,
    add_multiple_and_return_versions,
    verify_archive,
    verify_deleted,
    verify_deleted_archive,
    verify_row,
)


@pytest.fixture
def user_table():
    return MultiColumnUserTable


@pytest.fixture
def p1_dict():
    return dict(product_id_1=11, product_id_2='foo', col1='foo', col2=100)


@pytest.fixture
def p2_dict():
    return dict(product_id_1=11, product_id_2='bar', col1='foo', col2=100)


@pytest.fixture
def p3_dict():
    return dict(product_id_1=10, product_id_2='bar', col1='foo', col2=100)


@pytest.fixture
def delete_api_test_setup(session, p1, p2):
    add_multiple_and_return_versions([p1, p2], session)

    p1.col1 = 'change1'
    add_and_return_version(p1, session)

    p1.col1 = 'change2'
    add_and_return_version(p1, session)

    p1.col2 = 15
    p2.col2 = 12
    add_multiple_and_return_versions([p1, p2], session)


def test_insert(session, user_table, p1_dict, p1):
    version = add_and_return_version(p1, session)

    verify_row(p1_dict, version, session, user_table=user_table)
    verify_archive(p1_dict, version, session, user_table=user_table)


def test_multi_insert(session, user_table, p1_dict, p1, p2_dict, p2, p3_dict, p3):
    versions = add_multiple_and_return_versions([p1, p2, p3], session)

    # Assert the columns match
    expected = [p1_dict, p2_dict, p3_dict]
    for row, version in zip(expected, versions):
        verify_row(row, version, session, user_table=user_table)
        verify_archive(row, version, session, user_table=user_table)


def test_unique_constraint(session, user_table, p1):
    add_and_return_version(p1, session)

    invalid_p_dict = dict(product_id_1=11, product_id_2='foo', col1='bar', col2=100)
    invalid_p = user_table(**invalid_p_dict)
    with pytest.raises(IntegrityError):
        add_and_return_version(invalid_p, session)


def test_update(session, user_table, p1_dict, p1):
    add_and_return_version(p1, session)

    p1.col1 = 'bar'
    p1.col2 = 300
    version_2 = add_and_return_version(p1, session)
    version_2_dict = dict(p1_dict, col1='bar', col2=300)

    verify_row(version_2_dict, version_2, session, user_table=user_table)
    verify_archive(version_2_dict, version_2, session, user_table=user_table)


def test_multi_update(session, user_table, p1_dict, p1):
    add_and_return_version(p1, session)

    p1.col1 = 'bar'
    p1.col2 = 300
    version_2 = add_and_return_version(p1, session)
    version_2_dict = dict(p1_dict, col1='bar', col2=300)

    verify_row(version_2_dict, version_2, session, user_table=user_table)
    verify_archive(version_2_dict, version_2, session, user_table=user_table)

    p1.col1 = 'hello'
    p1.col2 = 404
    version_3 = add_and_return_version(p1, session)
    version_3_dict = dict(p1_dict, col1='hello', col2=404)

    verify_row(version_3_dict, version_3, session, user_table=user_table)
    verify_archive(version_3_dict, version_3, session, user_table=user_table)


def test_update_version_column(session, user_table, p1_dict, p1):
    version = add_and_return_version(p1, session)

    verify_row(p1_dict, version, session, user_table=user_table)
    verify_archive(p1_dict, version, session, user_table=user_table)

    p1.product_id_1 = 12
    version_2 = add_and_return_version(p1, session)
    version_2_dict = dict(p1_dict, product_id_1=12)

    verify_row(version_2_dict, version_2, session, user_table=user_table)
    verify_archive(version_2_dict, version_2, session, user_table=user_table)
    verify_archive(p1_dict, version_2, session, deleted=True, user_table=user_table)


def test_delete(session, user_table, p1_dict, p1):
    version = add_and_return_version(p1, session)

    session.delete(p1)
    session.commit()
    filter_kwargs = dict(product_id_1=p1.product_id_1, product_id_2=p1.product_id_2)
    assert not session.query(user_table).filter_by(**filter_kwargs).count()
    verify_archive(p1_dict, version, session, user_table=user_table)
    verify_deleted_archive(p1_dict, p1, version, session, user_table)


def test_insert_after_delete(session, user_table, p1_dict, p1):
    version = add_and_return_version(p1, session)

    session.delete(p1)
    session.commit()

    p_new = dict(p1_dict, **dict(product_id_1=11, product_id_2='foo', col1='new', col2=101))
    q = user_table(**p_new)
    new_version = add_and_return_version(q, session)

    verify_row(p_new, new_version, session, user_table=user_table)
    verify_archive(p1_dict, version, session, user_table=user_table)
    deleted_version = verify_deleted_archive(p1_dict, p1, version, session, user_table)
    verify_archive(p_new, new_version, session, user_table=user_table)
    assert new_version > deleted_version


def test_delete_single_row(session, user_table, delete_api_test_setup):
    conds = [{'product_id_1': 11, 'product_id_2': 'foo'}]
    delete(user_table, session, conds=conds)
    verify_deleted(conds[0], session, user_table=user_table)


def test_delete_multi_row(session, user_table, delete_api_test_setup):
    conds = [
        {'product_id_1': 11, 'product_id_2': 'bar'},
        {'product_id_1': 11, 'product_id_2': 'foo'}
    ]
    delete(user_table, session, conds=conds)
    for c in conds:
        verify_deleted(c, session, user_table=user_table)
