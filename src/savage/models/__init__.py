from datetime import datetime

from psycopg2.extensions import AsIs
from sqlalchemy import BigInteger, Boolean, Column, DateTime, insert, inspect, Integer, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm.attributes import InstrumentedAttribute

from savage import utils
from savage.exceptions import LogTableCreationError


def current_version_sql(as_is=False):
    sql_fn = 'txid_current()'
    if as_is:
        # NOTE: The AsIs construct allows raw SQL to be passed through in `SQLAlchemy.insert`
        return AsIs(sql_fn)
    return text(sql_fn)


class SavageLogMixin(object):
    """
    A mixin providing the schema for the log table, an append only table which saves old versions
    of rows. An inheriting model must specify the following columns:
      - user_id - a column corresponding to the user that made the specified change
      - 1 or more columns which are a subset of columns in the user table. These columns
      must have a unique constraint on the user table and also be named the same in both tables
    """
    archive_id = Column(Integer, primary_key=True, autoincrement=True)
    version_id = Column(BigInteger, nullable=False, index=True)
    deleted = Column(Boolean, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    data = Column(postgresql.JSONB, nullable=False)

    __mapper_args__ = {
        'version_id_col': version_id,
        'version_id_generator': False
    }

    @classmethod
    def build_row_dict(cls, row, dialect, deleted=False, user_id=None, use_dirty=True):
        """
        Builds a dictionary of archive data from row which is suitable for insert.

        NOTE: If `deleted` is False, version ID will be set to an AsIs SQL construct.

        :param row: instance of :class:`~SavageModelMixin`
        :param dialect: :py:class:`~sqlalchemy.engine.interfaces.Dialect`
        :param deleted: whether or not the row is deleted (defaults to False)
        :param user_id: ID of user that is performing the update on this row (defaults to None)
        :param use_dirty: whether to use the dirty fields from row or not (defaults to True)
        :return: a dictionary of archive table column names to values, suitable for insert
        :rtype: dict
        """
        data = {
            'data': row.to_archivable_dict(dialect, use_dirty=use_dirty),
            'deleted': deleted,
            'updated_at': datetime.now(),
            'version_id': current_version_sql(as_is=True) if deleted else row.version_id
        }
        for col_name in row.version_columns:
            data[col_name] = utils.get_column_attribute(row, col_name, use_dirty=use_dirty)
        if user_id is not None:
            data['user_id'] = user_id
        return data

    @classmethod
    def bulk_archive_rows(cls, rows, session, user_id=None, chunk_size=1000):
        """
        Bulk archives data previously written to DB.

        :param rows: iterable of previously saved model instances to archive
        :param session: DB session to use for inserts
        :param user_id: ID of user responsible for row modifications
        :return:
        """
        dialect = utils.get_dialect(session)
        to_insert_dicts = []
        for row in rows:
            row_dict = cls.build_row_dict(row, user_id=user_id, dialect=dialect)
            to_insert_dicts.append(row_dict)
            if len(to_insert_dicts) < chunk_size:
                continue

            # Insert a batch of rows
            session.execute(insert(cls).values(to_insert_dicts))
            to_insert_dicts = []

        # Insert final batch of rows (if any)
        if to_insert_dicts:
            session.execute(insert(cls).values(to_insert_dicts))
        session.commit()

    @classmethod
    def _validate(cls, engine, *version_cols):
        """
        Validates the archive table.

        Validates the following criteria:
            - all version columns exist in the archive table
            - the python types of the user table and archive table columns are the same
            - a user_id column exists
            - there is a unique constraint on version and the other versioned columns from the
            user table

        :param engine: instance of :class:`~sqlalchemy.engine.Engine`
        :param *version_cols: instances of :class:`~InstrumentedAttribute` from
        the user table corresponding to the columns that versioning pivots around
        :raises: :class:`~LogTableCreationError`
        """
        cls._version_col_names = set()
        for version_column_ut in version_cols:
            # Make sure all version columns exist on this table
            version_col_name = version_column_ut.key
            version_column_at = getattr(cls, version_col_name, None)
            if not isinstance(version_column_at, InstrumentedAttribute):
                raise LogTableCreationError("Log table needs {} column".format(version_col_name))

            # Make sure the type of the user table and log table columns are the same
            version_col_at_t = version_column_at.property.columns[0].type.__class__
            version_col_ut_t = version_column_ut.property.columns[0].type.__class__
            if version_col_at_t != version_col_ut_t:
                raise LogTableCreationError(
                    "Type of column {} must match in log and user table".format(version_col_name)
                )
            cls._version_col_names.add(version_col_name)

        # Ensure user added a user_id column
        # TODO: should user_id column be optional?
        user_id = getattr(cls, 'user_id', None)
        if not isinstance(user_id, InstrumentedAttribute):
            raise LogTableCreationError("Log table needs user_id column")

        # Check the unique constraint on the versioned columns
        version_col_names = list(cls._version_col_names) + ['version_id']
        if not utils.has_constraint(cls, engine, *version_col_names):
            raise LogTableCreationError("There is no unique constraint on the version columns")


class SavageModelMixin(object):
    version_id = Column(
        BigInteger,
        nullable=False,
        server_default=current_version_sql(),
        server_onupdate=current_version_sql()
    )

    __mapper_args__ = {
        'version_id_col': version_id,
        'version_id_generator': False
    }

    ignore_columns = None
    version_columns = None

    @classmethod
    def register(cls, archive_table, engine):
        """
        :param archive_table: the model for the users archive table
        :param engine: the database engine
        :param version_col_names: strings which correspond to columns that versioning will pivot \
            around. These columns must have a unique constraint set on them.
        """
        version_col_names = cls.version_columns
        if not version_col_names:
            raise LogTableCreationError('Need to specify version cols in cls.version_columns')
        if cls.ignore_columns is None:
            cls.ignore_columns = set()
        cls.ignore_columns.add('version_id')
        version_cols = [getattr(cls, col_name, None) for col_name in version_col_names]

        cls._validate(engine, *version_cols)

        archive_table._validate(engine, *version_cols)
        cls.ArchiveTable = archive_table

    def updated_by(self, user):
        self._updated_by = user

    def update_version_id(self):
        self.version_id = current_version_sql()

    def to_archivable_dict(self, dialect, use_dirty=True):
        """
        :param dialect: a :py:class:`~sqlalchemy.engine.interfaces.Dialect` corresponding to the \
            SQL dialect being used.
        :param use_dirty: whether to make a dict of the fields as they stand, or the fields \
            before the row was updated

        :return: a dictionary of key value pairs representing this row.
        :rtype: dict
        """
        return {
            cn: utils.get_column_attribute(self, c, use_dirty=use_dirty, dialect=dialect)
            for c, cn in utils.get_column_keys_and_names(self)
            if c not in self.ignore_columns
        }

    @classmethod
    def _validate(cls, engine, *version_cols):
        version_col_names = set()
        for version_column_ut in version_cols:
            if not isinstance(version_column_ut, InstrumentedAttribute):
                raise LogTableCreationError("All version columns must be <InstrumentedAttribute>")
            version_col_names.add(version_column_ut.key)

        # Check the unique constraint on the versioned columns
        insp = inspect(cls)
        uc = sorted([col.name for col in insp.primary_key]) == sorted(version_col_names)
        if not (uc or utils.has_constraint(cls, engine, *version_col_names)):
            raise LogTableCreationError("There is no unique constraint on the version columns")
