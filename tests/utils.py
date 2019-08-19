from __future__ import absolute_import

import sqlalchemy as sa
from sqlalchemy import func

from savage import utils
from tests.models import UserTable


def add_and_return_version(row, session):
    session.add(row)
    session.commit()
    return row.version_id


def add_multiple_and_return_versions(rows, session):
    session.add_all(rows)
    session.commit()
    return [r.version_id for r in rows]


def verify_archive(expected, version, session, deleted=False, user=None, user_table=None):
    UserTable_ = user_table or UserTable
    ArchiveTable_ = UserTable_.ArchiveTable

    and_clause = sa.and_(ArchiveTable_.version_id == version, *(
        getattr(ArchiveTable_, col_name) == expected[col_name]
        for col_name in UserTable_.version_columns
    ))
    res = session.execute(
        sa.select([ArchiveTable_]).
        where(and_clause)
    )
    all_ = utils.result_to_dict(res)
    assert len(all_) == 1
    row = all_[0]
    data = row['data']
    assert bool(row['deleted']) is deleted
    if user is not None:
        assert row['user_id'] == user
    for k in expected:
        assert k in data
        assert data[k] == expected[k]


def verify_deleted_archive(row_dict, row, version, session, user_table, user=None):
    archive_table = user_table.ArchiveTable
    and_clause = sa.and_(
        archive_table.version_id > version,
        archive_table.deleted.is_(True),
        *[
            getattr(archive_table, col_name) == getattr(row, col_name)
            for col_name in user_table.version_columns
        ]
    )
    deleted_archive_rows = session.query(archive_table).filter(and_clause).all()
    assert len(deleted_archive_rows) == 1
    deleted_version = deleted_archive_rows[0].version_id
    kwargs = dict(deleted=True, user=user, user_table=user_table)
    verify_archive(row_dict, deleted_version, session, **kwargs)
    return deleted_version


def verify_row(expected_dict, version, session, user_table=None):
    UserTable_ = user_table or UserTable

    # Query user table and assert there is exactly 1 row
    and_clause = sa.and_(*[
        getattr(UserTable_, col_name) == expected_dict[col_name]
        for col_name in UserTable_.version_columns
    ])
    res = session.execute(
        sa.select([UserTable_]).
        where(and_clause)
    )
    all_ = utils.result_to_dict(res)
    assert len(all_) == 1
    row_dict = all_[0]

    # Assert the columns match
    assert row_dict['version_id'] == version
    for k in expected_dict:
        assert row_dict[k] == expected_dict[k]


def verify_deleted(key, session, user_table=None):
    UserTable_ = user_table or UserTable
    ArchiveTable_ = UserTable_.ArchiveTable
    version_col_names = UserTable_.version_columns
    assert len(key) == len(version_col_names)

    and_clause = sa.and_(*[
        getattr(ArchiveTable_, col_name) == key[col_name]
        for col_name in version_col_names
    ])
    res = session.execute(
        sa.select([func.count(ArchiveTable_.archive_id)])
        .where(and_clause)
    )
    assert res.scalar() == 0

    and_clause = sa.and_(*[
        getattr(UserTable_, col_name) == key[col_name]
        for col_name in version_col_names
    ])
    res = session.execute(
        sa.select([func.count(UserTable_.id)])
        .where(and_clause)
    )
    assert res.scalar() == 0
