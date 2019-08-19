from __future__ import absolute_import

from datetime import datetime

import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base

from savage.exceptions import LogTableCreationError
from savage.models import SavageLogMixin
from tests.models import ArchiveTable, UserTable
from tests.utils import add_and_return_version


def test_register_bad_archive_table_fails(engine):
    """
    Assert that an archive table with the following conditions fails to get registered:
        * no product_id column
        * the product_id column has the wrong type
    """
    Base_ = declarative_base()  # use alternate base so these tables don't get created
    try:
        # no product_id column
        class ArchiveNoFKey(SavageLogMixin, Base_):
            __tablename__ = 'no_fkey'
            user_id = Column(String(50))
        with pytest.raises(LogTableCreationError):
            UserTable.register(ArchiveNoFKey, engine)

        # product_id is not the same type as product_id
        class ArchiveWrongFKey(SavageLogMixin, Base_):
            __tablename__ = 'wrong_fkey'
            product_id = Column(String(10))
            user_id = Column(String(50))
        with pytest.raises(LogTableCreationError):
            UserTable.register(ArchiveWrongFKey, engine)

        # column is named something different
        class ArchiveWrongName(SavageLogMixin, Base_):
            __tablename__ = 'wrong_name'
            foo = Column(String(10))
            user_id = Column(String(50))
        with pytest.raises(LogTableCreationError):
            UserTable.register(ArchiveWrongName, engine)

        # user did not add user_id column
        class ArchiveNoUserId(SavageLogMixin, Base_):
            __tablename__ = 'no_user_id'
            product_id = Column(Integer, nullable=False)
        with pytest.raises(LogTableCreationError):
            UserTable.register(ArchiveNoUserId, engine)

        # no unique constraint on version column
        class ArchiveNoConstraint(SavageLogMixin, Base_):
            __tablename__ = 'no_constraint'
            product_id = Column(Integer)
            user_id = Column(String(50))
        with pytest.raises(LogTableCreationError):
            UserTable.register(ArchiveNoConstraint, engine)
    finally:
        UserTable.register(ArchiveTable, engine)


def test_archive_table_collision_fails_1(session, user_table, p1):
    """
    Try to insert two records with the same version and foreign key in the same transaction
    and ensure the write fails. In other words, ensure the unique constraint
    is correctly imposed on the archive table.
    """
    # Insert an element so it exists in the archive table
    add_and_return_version(p1, session)

    to_insert = {
        'deleted': False,
        'user_id': 'bar',
        'updated_at': datetime.now(),
        'data': {},
        'product_id': p1.product_id,
    }
    session.add(user_table.ArchiveTable(**to_insert))
    to_insert = {
        'deleted': True,
        'user_id': 'foo',
        'updated_at': datetime.now(),
        'data': {},
        'product_id': p1.product_id,
    }
    session.add(user_table.ArchiveTable(**to_insert))
    with pytest.raises(IntegrityError):
        session.flush()
