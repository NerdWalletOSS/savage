from __future__ import absolute_import

import copy
import json
from datetime import datetime
from itertools import chain

import pytest
import sqlalchemy as sa
from six.moves import range, zip
from sqlalchemy import func

from savage.api import delete, get
from savage.api.data import _get_conditions_list
from savage.utils import get_dialect, savage_json_serializer
from tests.utils import add_and_return_version, add_multiple_and_return_versions, verify_deleted


@pytest.fixture
def t1():
    return datetime.utcfromtimestamp(10)


@pytest.fixture
def t2():
    return datetime.utcfromtimestamp(20)


@pytest.fixture
def t3():
    return datetime.utcfromtimestamp(30)


@pytest.fixture
def t4():
    return datetime.utcfromtimestamp(40)


@pytest.fixture
def get_api_test_setup(mocker, session, p1, p2, p3, t1, t2, t3, t4):
    mock_datetime = mocker.patch("savage.models.datetime")
    p1_history, p2_history, p3_history = [], [], []

    mock_datetime.utcnow.return_value = t1
    versions = add_multiple_and_return_versions([p1, p3], session)
    p1_history.append(_history(p1, t1, versions[0], session))
    p3_history.append(_history(p3, t1, versions[1], session))

    p1.col1 = "change1"
    mock_datetime.utcnow.return_value = t2
    versions = add_multiple_and_return_versions([p1, p2], session)
    p1_history.append(_history(p1, t2, versions[0], session))
    p2_history.append(_history(p2, t2, versions[1], session))

    p1.col3 = False
    p1.col1 = "change2"
    mock_datetime.utcnow.return_value = t3
    version = add_and_return_version(p1, session)
    p1_history.append(_history(p1, t3, version, session))

    p1.col2 = 15
    p2.col2 = 12
    mock_datetime.utcnow.return_value = t4
    versions = add_multiple_and_return_versions([p1, p2], session)
    p1_history.append(_history(p1, t4, versions[0], session))
    p2_history.append(_history(p2, t4, versions[1], session))

    return [p1_history, p2_history, p3_history]


@pytest.fixture
def delete_api_test_setup(session, p1, p2, p3):
    add_multiple_and_return_versions([p1, p3], session)

    p1.col1 = "change1"
    add_multiple_and_return_versions([p1, p2], session)

    p1.col3 = False
    p1.col1 = "change2"
    add_and_return_version(p1, session)

    p1.col2 = 15
    p2.col2 = 12
    add_multiple_and_return_versions([p1, p2], session)


def _history(row, ts, version, session):
    assert row.version_id == version
    d = row.to_archivable_dict(get_dialect(session))
    # Encode/decode JSON to ensure correct formatting
    d = json.loads(savage_json_serializer(d))
    return {
        "data": d,
        "updated_at": ts,
        "deleted": False,
        "version_id": version,
        "product_id": row.product_id,
    }


def assert_result(result, expected, fields=None):
    assert len(result) == len(expected)
    for res, exp in zip(result, expected):
        res = copy.deepcopy(res)
        exp = copy.deepcopy(exp)
        del res["user_id"]
        del res["archive_id"]
        res["data"].pop("id", None)
        del exp["data"]["id"]
        if fields is not None:
            for k in list(exp["data"].keys()):
                if k not in fields:
                    del exp["data"][k]

        assert res == exp


def test_delete_single_row(session, user_table, delete_api_test_setup):
    conds = [{"product_id": 10}]
    delete(user_table, session, conds=conds)
    verify_deleted(conds[0], session)


def test_delete_multi_row(session, user_table, delete_api_test_setup):
    conds = [{"product_id": 11}, {"product_id": 10}]
    delete(user_table, session, conds=conds)
    for c in conds:
        verify_deleted(c, session)


def test_delete_rollback(mocker, session, user_table, delete_api_test_setup):
    conds = [{"product_id": 10}]
    cond_list_1 = _get_conditions_list(user_table, conds)
    with mocker.patch(
        "savage.api.data._get_conditions_list", side_effect=[cond_list_1, Exception()]
    ):
        with pytest.raises(Exception):
            delete(user_table, session, conds=conds)

        version_col_names = user_table.version_columns
        and_clause = sa.and_(
            *[
                getattr(user_table.ArchiveTable, col_name) == conds[0][col_name]
                for col_name in version_col_names
            ]
        )
        res = session.execute(
            sa.select([func.count(user_table.ArchiveTable.archive_id)]).where(and_clause)
        )
        assert res.scalar() == 4

        and_clause = sa.and_(
            *[getattr(user_table, col_name) == conds[0][col_name] for col_name in version_col_names]
        )
        res = session.execute(sa.select([func.count(user_table.id)]).where(and_clause))
        assert res.scalar() == 1


def test_get_single_product_no_change(session, user_table, t1, get_api_test_setup):
    """Performs a query for p3 which has no changes for current time, previous time slice,
    a time period that includes t1, and a time period that does not include t1.
    """
    p1_history, p2_history, p3_history = get_api_test_setup
    conds = [{"product_id": 2546}]
    result = get(user_table, session, conds=conds)
    assert_result(result, p3_history)

    result = get(user_table, session, t1=datetime.utcfromtimestamp(5), conds=conds)
    assert not result

    result = get(user_table, session, t1=datetime.utcfromtimestamp(15), conds=conds)
    assert_result(result, p3_history)

    result = get(user_table, session, t1=t1, conds=conds)
    assert_result(result, p3_history)

    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(5),
        t2=datetime.utcfromtimestamp(11),
        conds=conds,
    )
    assert_result(result, p3_history)

    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(11),
        t2=datetime.utcfromtimestamp(15),
        conds=conds,
    )
    assert not result


def test_get_single_product_with_change(session, user_table, get_api_test_setup):
    """Performs a query for p1 which has been changed 3 times for current time, previous time
    slices, and various time periods.
    """
    p1_history, p2_history, p3_history = get_api_test_setup
    conds = [{"product_id": 10}]
    result = get(user_table, session, conds=conds)
    assert_result(result, p1_history[-1:])

    result = get(user_table, session, t1=datetime.utcfromtimestamp(15), conds=conds)
    assert_result(result, p1_history[:1])

    result = get(user_table, session, t1=datetime.utcfromtimestamp(35), conds=conds)
    assert_result(result, p1_history[2:3])

    result = get(user_table, session, t2=datetime.utcfromtimestamp(35), conds=conds)
    assert_result(result, p1_history[:3])

    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(11),
        t2=datetime.utcfromtimestamp(45),
        conds=conds,
    )
    assert_result(result, p1_history[1:])

    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(11),
        t2=datetime.utcfromtimestamp(35),
        conds=conds,
    )
    assert_result(result, p1_history[1:3])


def test_get_multiple_products(session, user_table, get_api_test_setup):
    p1_history, p2_history, p3_history = get_api_test_setup
    conds = [{"product_id": 10}, {"product_id": 11}]
    result = get(user_table, session, conds=conds)
    assert_result(result, [p1_history[-1], p2_history[-1]])

    result = get(user_table, session, t1=datetime.utcfromtimestamp(15), conds=conds)
    assert_result(result, p1_history[:1])

    result = get(user_table, session, t1=datetime.utcfromtimestamp(25), conds=conds)
    assert_result(result, [p1_history[1], p2_history[0]])

    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(11),
        t2=datetime.utcfromtimestamp(45),
        conds=conds,
    )
    assert_result(result, list(chain(p1_history[1:], p2_history)))


def test_get_all_products(session, user_table, get_api_test_setup):
    p1_history, p2_history, p3_history = get_api_test_setup
    result = get(user_table, session)
    assert_result(result, [p1_history[-1], p2_history[-1], p3_history[-1]])

    result = get(user_table, session, t1=datetime.utcfromtimestamp(31))
    assert_result(result, [p1_history[2], p2_history[0], p3_history[0]])

    result = get(user_table, session, t1=datetime.utcfromtimestamp(11))
    assert_result(result, [p1_history[0], p3_history[0]])

    result = get(
        user_table, session, t1=datetime.utcfromtimestamp(11), t2=datetime.utcfromtimestamp(45)
    )
    assert_result(result, list(chain(p1_history[1:], p2_history)))


def test_get_products_after_version(session, user_table, get_api_test_setup):
    p1_history, p2_history, p3_history = get_api_test_setup
    result = get(user_table, session, version_id=p1_history[0]["version_id"])
    assert_result(result, p1_history[1:] + p2_history)


def test_fields_query(session, user_table, get_api_test_setup):
    """Test specifying fields and make sure dedup happens correctly.
    """
    p1_history, p2_history, p3_history = get_api_test_setup
    conds = [{"product_id": 10}]

    fields = ["col2"]
    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(9),
        t2=datetime.utcfromtimestamp(45),
        conds=conds,
        fields=fields,
    )
    expected = [p1_history[0], p1_history[3]]
    assert_result(result, expected, fields=fields)

    fields = ["col1", "col2"]
    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(9),
        t2=datetime.utcfromtimestamp(45),
        conds=conds,
        fields=fields,
    )
    assert_result(result, p1_history, fields=fields)

    fields = ["col1"]
    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(9),
        t2=datetime.utcfromtimestamp(45),
        fields=fields,
    )
    assert_result(result, list(chain(p1_history[:3], p2_history[:1], p3_history)), fields=fields)

    fields = ["col1", "col2"]
    result = get(user_table, session, t1=datetime.utcfromtimestamp(11), conds=conds, fields=fields)
    assert_result(result, p1_history[:1], fields=fields)

    fields = ["col1", "col2"]
    result = get(user_table, session, conds=conds, fields=fields)
    assert_result(result, p1_history[-1:], fields=fields)

    fields = ["col1", "invalid_col"]
    result = get(user_table, session, conds=conds, fields=fields)
    p1_history[-1]["data"]["invalid_col"] = None
    assert_result(result, p1_history[-1:], fields=fields)


def test_failure_conditions(session, user_table, get_api_test_setup):
    """Pass invalid conds arguments and ensure the query fails.
    """
    conds = [{"product_id": 10, "foo": 15}]
    with pytest.raises(ValueError):
        get(user_table, session, t1=datetime.utcfromtimestamp(31), conds=conds)

    conds = [{"pid": 10}]
    with pytest.raises(ValueError):
        get(user_table, session, t1=datetime.utcfromtimestamp(31), conds=conds)

    with pytest.raises(ValueError):
        get(user_table, session, page=-10)


def test_paging_results(mocker, session, user_table, p1_dict, p1):
    t = datetime.utcfromtimestamp(10000)
    mock_datetime = mocker.patch("savage.models.datetime")
    mock_datetime.utcnow.return_value = t
    history = []
    p1.col2 = 0
    version = add_and_return_version(p1, session)
    history.append(_history(p1, t, version, session))
    # make 500 changes
    for i in range(500):
        p1.col1 = "foobar" + "1" * ((i + 1) // 10)
        p1.col2 += 1
        p1.col3 = i < 250
        version = add_and_return_version(p1, session)
        history.append(_history(p1, t, version, session))
    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(0),
        t2=datetime.utcfromtimestamp(10000000000),
        page=1,
        page_size=1000,
    )
    assert_result(result, history)
    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(0),
        t2=datetime.utcfromtimestamp(10000000000),
        page=1,
        page_size=100,
    )
    assert_result(result, history[:100])
    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(0),
        t2=datetime.utcfromtimestamp(10000000000),
        page=3,
        page_size=100,
    )
    assert_result(result, history[200:300])
    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(0),
        t2=datetime.utcfromtimestamp(10000000000),
        page=5,
        page_size=100,
    )
    assert_result(result, history[400:500])
    result = get(
        user_table,
        session,
        t1=datetime.utcfromtimestamp(0),
        t2=datetime.utcfromtimestamp(10000000000),
        fields=["col1"],
        page=1,
        page_size=80,
    )
    assert_result(result, history[0:80:10], fields=["col1"])
