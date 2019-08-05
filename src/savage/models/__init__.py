import logging
from datetime import datetime

import arrow
import sqlalchemy as sa
from psycopg2.extensions import AsIs
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    func,
    Index,
    insert,
    inspect,
    Integer,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm.attributes import InstrumentedAttribute

from savage import utils
from savage.exceptions import LogTableCreationError, RestoreError, LogIdentifyError, HistoryItemNotFound

log = logging.getLogger(__name__)


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
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    data = Column(postgresql.JSONB, nullable=False)

    __mapper_args__ = {
        'eager_defaults': True,  # Avoid unnecessary select to fetch updated_at
        'version_id_col': version_id,
        'version_id_generator': False
    }

    @declared_attr
    def __table_args__(cls):
        return (
            Index('index_{}_on_data_gin'.format(cls.__tablename__), 'data', postgresql_using='gin'),
        )

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
    def bulk_archive_rows(cls, rows, session, user_id=None, chunk_size=1000, commit=True):
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
        if commit:
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
        onupdate=current_version_sql()
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

    def version(self, session):
        """
        Returns the rows current version. This can only be called after a row has been
        inserted into the table and the session has been flushed. Otherwise this
        method has undefined behavior.
        """
        result = session.execute(
            sa.select([self.ArchiveTable.version_id]).
                where(self.ArchiveTable.archive_id == self.archive_id)
        ).first()
        return result[0]

    @classmethod
    def create_log_select_expression(cls, attributes):
        expressions = ()
        for col_name in cls.ArchiveTable._version_col_names:
            if col_name not in attributes:
                raise LogIdentifyError("Can't determine item id - no parameters passed, "
                                       "please pass '{}' argument".format(col_name))

            expressions += (getattr(cls.ArchiveTable, col_name) == attributes[col_name],)

        if len(expressions) > 1:
            return and_(expressions)

        return expressions[0]

    @classmethod
    def version_list_by_pk(cls, session, **kwargs):
        """
        Returns all VA version id's of this record with there corresponding user_id.
        This can be called after a row has been inserted into the table and the session has been flushed.
        """
        return utils.result_to_dict(session.execute(
            sa.select([cls.ArchiveTable.archive_id, cls.ArchiveTable.user_id, cls.ArchiveTable.version_id])
                .where(cls.create_log_select_expression(kwargs))
        ))

    def get_row_identifier(self):
        return {
            col_name: getattr(self, col_name) for col_name in self.ArchiveTable._version_col_names
        }

    def version_list(self, session):
        """
        Returns all VA version id's of this record with there corresponding user_id.
        This can be called after a row has been inserted into the table and the session has been flushed.
        :param session: flushed session
        :return: a list of dicts with archive_id and user_id as keys and their values
        :rtype: list
        """
        return self.version_list_by_pk(session, **self.get_row_identifier())

    @classmethod
    def version_get(cls, session, version_id=None, archive_id=None):
        """
        Returns historic object (log record). Provide one of version_id, archive_id to identify version
        This can be called after a row has been inserted into the table and the session has been flushed.
        :param session: flushed session
        :param version_id - version_id of log row (version_id field in version_id)
        :param archive_id: archive_id of requested record (archive_id field). Can be used as alternative to version_id
        :return: a dictionary of key value pairs representing version id, id of the record in model,
         and versioned model's data
        :rtype: dict
        """
        if version_id is not None and archive_id is not None:
            log.warning(
                "both version_id and archive_id provided, only version_id will be used, please exclude one of them "
                "from call")

        if version_id is None and archive_id is None:
            raise LogIdentifyError("Please provide at least one from version_id, archive_id to identify column")

        if version_id is not None:
            filter_condition = (cls.ArchiveTable.version_id == version_id,)
        else:
            filter_condition = (cls.ArchiveTable.archive_id == archive_id,)

        result = utils.result_to_dict(session.execute(
            sa.select({cls.ArchiveTable.archive_id, cls.ArchiveTable.data})
                .where(*filter_condition)
        ))

        if not len(result):
            if version_id is not None:
                identify_str = 'version_id={}'.format(version_id)
            else:
                identify_str = 'archive_id={}'.format(archive_id)
            raise HistoryItemNotFound("Can't find log record by {}".format(identify_str))

        result = result[0]
        historic_object = result['data']
        historic_object['archive_id'] = result['archive_id']
        return historic_object

    @classmethod
    def version_restore(cls, session, version_id=None, archive_id=None):
        """
        Restores historic object. Provide one of version_id, archive_id to identify version
        If column was  not included in older version, then it should be nullable. \
        This method will set new value to null. Otherwise, it rases exception.
        :param session: flushed session
        :param version_id - version_id of log row to restore (version_id field in version_id)
        :param archive_id: archive_id of record to restore (archive_id field). Can be used as alternative to version_id
        :return: None
        """
        vals = cls.version_get(session, version_id, archive_id)
        row = session.query(cls).get(vals['id'])
        values = {}
        for col_name, model_column in cls.__dict__.items():
            if type(model_column) is not InstrumentedAttribute:
                continue
            if col_name in vals:
                values[col_name] = vals.get(col_name)
                if values[col_name] is not None and getattr(cls, col_name).type.python_type is datetime:
                    values[col_name] = arrow.get(vals[col_name]).datetime
            else:
                if getattr(model_column, 'nullable', None) is not None:
                    if model_column.nullable:
                        values[col_name] = None
                        log.warning("Model '{}' has new column '{}' which has no default, using NULL".format(
                            cls.__name__, col_name))
                    else:
                        raise RestoreError(
                            ("We does not support non-nullable values that were added in new version of model"
                             "'{}'. New column is '{}', please mark it as nullable to be able to restore").format(
                                cls.__name__, col_name))
        if row is None:
            session.execute(sa.insert(cls).values(values))
        else:
            for col_name, col_value in values.items():
                setattr(row, col_name, col_value)
        session.flush()
        session.commit()

    @classmethod
    def version_diff(cls, session, version_id=None, archive_id=None):
        """
        Compares version identified by 'version_id' or 'archive_id' with previous version.
        Provide one of version_id, archive_id
        :param session: flushed session
        :param archive_id: version id of log row to be compared
        :return: dict with versions, user_id's and dict with columns as keys and changes as values
        :return: dict
        """
        if version_id is not None and archive_id is not None:
            log.warning(
                "both version_id and archive_id provided, only version_id will be used, please exclude one of them "
                "from call")

        if version_id is None and archive_id is None:
            raise LogIdentifyError("Please provide at least one from version_id, archive_id to identify column")

        if version_id is not None:
            filter_condition = (cls.ArchiveTable.version_id == version_id,)
        else:
            filter_condition = (cls.ArchiveTable.archive_id == archive_id,)

        this_row = utils.result_to_dict(session.execute(
            sa.select({cls.ArchiveTable}).where(*filter_condition)
        ))
        if not len(this_row):
            if version_id is not None:
                identify_str = 'version_id={}'.format(version_id)
            else:
                identify_str = 'archive_id={}'.format(archive_id)
            raise HistoryItemNotFound("Can't find log record by {}".format(identify_str))
        this_row = this_row[0]

        archive_id = this_row['archive_id']
        all_history_items = {
            col_name: this_row[col_name] for col_name in cls.ArchiveTable._version_col_names
        }
        prev_log = [
            log for log in cls.version_list_by_pk(session, **all_history_items) if log['archive_id'] < archive_id
        ]
        if not prev_log:
            return utils.compare_rows(None, this_row)

        prev_archive_id = prev_log[-1]['archive_id']
        prev_row = utils.result_to_dict(session.execute(
            sa.select({cls.ArchiveTable})
                .where(cls.ArchiveTable.archive_id == prev_archive_id)
        ))[0]

        return utils.compare_rows(prev_row, this_row)

    def version_diff_all(self, session):
        return self.version_diff_all_by_pk(session, **self.get_row_identifier())

    @classmethod
    def version_diff_all_by_pk(cls, session, **kwargs):
        all_history_items = utils.result_to_dict(session.execute(
            sa.select([cls.ArchiveTable])
                .where(cls.create_log_select_expression(kwargs))
        ))
        all_changes = []
        for i in range(len(all_history_items)):
            if i is 0:
                all_changes.append(utils.compare_rows(None, all_history_items[i]))
            else:
                all_changes.append(utils.compare_rows(all_history_items[i - 1], all_history_items[i]))

        return all_changes

    @classmethod
    def version_get_all_by_pk(cls, session, **kwargs):
        all_history_items = utils.result_to_dict(session.execute(
            sa.select([
                cls.ArchiveTable.archive_id,
                cls.ArchiveTable.version_id,
                cls.ArchiveTable.user_id,
                cls.ArchiveTable.data.label('record')
            ]).where(
                cls.create_log_select_expression(kwargs))
        ))
        return all_history_items

    def version_get_all(self, session):
        return self.version_get_all_by_pk(session, **self.get_row_identifier())
