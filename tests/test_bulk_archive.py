from __future__ import absolute_import

import pytest
from six.moves import zip

from tests.models import ArchiveTable, UserTable
from tests.utils import verify_archive, verify_row


@pytest.fixture
def row_dicts(p1_dict, p2_dict, p3_dict):
    return [p1_dict, p2_dict, p3_dict]


@pytest.fixture
def rows_to_archive(session, row_dicts):
    session.bulk_insert_mappings(UserTable, row_dicts)
    return session.query(UserTable).all()


@pytest.fixture
def row_versions(rows_to_archive):
    return [r.version_id for r in rows_to_archive]


def test_bulk_archive_rows(session, row_dicts, rows_to_archive, row_versions):
    ArchiveTable.bulk_archive_rows(rows_to_archive, session)
    for row, version in zip(row_dicts, row_versions):
        verify_row(row, version, session)
        verify_archive(row, version, session)


def test_bulk_archive_rows_with_user(session, row_dicts, rows_to_archive, row_versions):
    user_id = "test_user"
    ArchiveTable.bulk_archive_rows(rows_to_archive, session, user_id=user_id)
    for row, version in zip(row_dicts, row_versions):
        verify_row(row, version, session)
        verify_archive(row, version, session)


def test_bulk_archive_rows_chunk_size(mocker, session, rows_to_archive):
    mocker.spy(session, "execute")
    ArchiveTable.bulk_archive_rows(rows_to_archive, session, chunk_size=1)  # Ensure three chunks
    assert session.execute.call_count == 3


def test_bulk_archive_rows_commit_false(mocker, session, rows_to_archive):
    mocker.spy(session, "commit")
    ArchiveTable.bulk_archive_rows(rows_to_archive, session, commit=False)
    assert not session.commit.called
