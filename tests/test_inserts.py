from __future__ import absolute_import

from six.moves import zip

import savage
from tests.utils import (
    add_and_return_version,
    add_multiple_and_return_versions,
    verify_archive,
    verify_row,
)


def test_insert_new_product(session, p1_dict, p1):
    assert savage.is_initialized()
    p1.col4 = 11
    version = add_and_return_version(p1, session)

    expected = dict(other_name=11, **p1_dict)
    verify_row(expected, version, session)
    verify_archive(expected, version, session)


def test_insert_multiple_products(session, p1_dict, p1, p2_dict, p2, p3_dict, p3):
    versions = add_multiple_and_return_versions([p1, p2, p3], session)

    # Assert the columns match
    expected = [p1_dict, p2_dict, p3_dict]
    for row, version in zip(expected, versions):
        verify_row(row, version, session)
        verify_archive(row, version, session)


def test_insert_new_product_with_user(session, p1_dict, p1):
    p1.updated_by('test_user')
    version = add_and_return_version(p1, session)

    verify_row(p1_dict, version, session)
    verify_archive(p1_dict, version, session, user='test_user')


def test_insert_new_product_with_json(session, p1_dict, p1):
    json_dict = {'foo': 'bar'}
    p1.jsonb_col = json_dict.copy()
    version = add_and_return_version(p1, session)

    expected = dict(jsonb_col=json_dict, **p1_dict)
    verify_row(expected, version, session)
    verify_archive(expected, version, session)
