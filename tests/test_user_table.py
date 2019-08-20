from __future__ import absolute_import

import pytest
from sqlalchemy import Column, Integer, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

from savage import SavageModelMixin
from savage.exceptions import LogTableCreationError
from savage.models import SavageLogMixin
from tests.models import ArchiveTable, UserTable


def test_register_user_table(engine):
    Base_ = declarative_base()
    try:
        # --- Test failure cases ---
        class WrongVersionColumns(SavageModelMixin, Base_):
            __tablename__ = "wrong_version_cols"
            version_columns = ["id"]
            pid = Column(Integer, primary_key=True)

        class WrongVersionColumnsArchive(SavageLogMixin, Base_):
            __tablename__ = "wrong_version_cols_archive"
            pid = Column(Integer)

        with pytest.raises(LogTableCreationError):
            WrongVersionColumns.register(WrongVersionColumnsArchive, engine)

        class NoVersionCols(SavageModelMixin, Base_):
            __tablename__ = "no_version_cols"
            pid = Column(Integer, primary_key=True)

        class NoVersionColsArchive(SavageLogMixin, Base_):
            __tablename__ = "no_version_cols_archive"
            pid = Column(Integer)

        with pytest.raises(LogTableCreationError):
            NoVersionCols.register(NoVersionColsArchive, engine)

        class NoConstraint(SavageModelMixin, Base_):
            __tablename__ = "no_constraint"
            version_columns = ["pid1", "pid2"]
            pid1 = Column(Integer, primary_key=True)
            pid2 = Column(Integer)

        class NoConstraintArchive(SavageLogMixin, Base_):
            __tablename__ = "no_constraint_archive"
            pid1 = Column(Integer)
            pid2 = Column(Integer)

        with pytest.raises(LogTableCreationError):
            NoConstraint.register(NoConstraintArchive, engine)

        # --- Test success cases ---
        class PKConstraint(SavageModelMixin, Base_):
            __tablename__ = "pk_constraint"
            version_columns = ["pid1"]
            pid1 = Column(Integer, primary_key=True)
            pid2 = Column(Integer)

        class PKConstraintArchive(SavageLogMixin, Base_):
            __tablename__ = "pk_constraint_archive"
            pid1 = Column(Integer)
            user_id = Column(Integer)
            __table_args__ = (UniqueConstraint("pid1", "version_id", name="pid"),)

        Base_.metadata.create_all(engine)
        PKConstraint.register(PKConstraintArchive, engine)
    finally:
        UserTable.register(ArchiveTable, engine)
        Base_.metadata.drop_all(engine)


def test_insert_into_unregistered_table_fails(engine, session):
    Base_ = declarative_base()

    class UnregisteredTable(SavageModelMixin, Base_):
        __tablename__ = "unregistered_table"
        pid = Column(Integer, primary_key=True)
        col1 = Column(Integer)

    Base_.metadata.create_all(engine)
    session.add(UnregisteredTable(pid=1, col1=5))
    try:
        with pytest.raises(LogTableCreationError):
            session.commit()
    finally:
        Base_.metadata.drop_all(engine)
