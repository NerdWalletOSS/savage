import datetime

from sqlalchemy import Boolean, Column, DateTime, Integer, String, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

from savage.models import SavageLogMixin, SavageModelMixin

Base = declarative_base()


class UserTable(SavageModelMixin, Base):
    __tablename__ = 'test_table'
    version_columns = ['product_id']
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, unique=True, nullable=False)
    col1 = Column(String(100))
    col2 = Column(Integer)
    col3 = Column(Boolean)
    col4 = Column('other_name', Integer)
    col5 = Column(DateTime, default=datetime.datetime.utcnow)


class ArchiveTable(SavageLogMixin, Base):
    __tablename__ = 'test_table_archive'
    product_id = Column(Integer, nullable=False)
    user_id = Column(String(100))

    __table_args__ = (
        UniqueConstraint('product_id', 'version_id', name='unique_on_product_id_and_version'),
    )


class MultiColumnUserTable(SavageModelMixin, Base):
    __tablename__ = 'multi_col_test_table'
    version_columns = ['product_id_1', 'product_id_2']
    id = Column(Integer, primary_key=True)
    product_id_1 = Column(Integer, index=True, nullable=False)
    product_id_2 = Column(String(100), index=True, nullable=False)
    col1 = Column(String(100))
    col2 = Column(Integer)

    __table_args__ = (
        UniqueConstraint('product_id_1', 'product_id_2', name='unique_on_product_ids'),
    )


class MultiColumnArchiveTable(SavageLogMixin, Base):
    __tablename__ = 'multi_col_test_table_archive'
    product_id_1 = Column(Integer, nullable=False)
    product_id_2 = Column(String(100), index=True, nullable=False)
    user_id = Column(String(100))
    __table_args__ = (
        UniqueConstraint(
            'product_id_1', 'product_id_2', 'version_id', name='unique_on_product_ids_and_version'
        ),
    )


class UnarchivedTable(Base):
    __tablename__ = 'unlogged_test_table'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    _private_attr = Column('private_attr', Integer)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
