from datetime import datetime

import sqlalchemy as sa

from savage import utils


def delete(table, session, conds):
    """Performs a hard delete on a row, which means the row is deleted from the Savage
    table as well as the archive table.

    :param table: the model class which inherits from
        :class:`~savage.models.user_table.SavageModelMixin` and specifies the model
        of the user table from which we are querying
    :param session: a sqlalchemy session with connections to the database
    :param conds: a list of dictionary of key value pairs where keys are columns in the table
        and values are values the column should take on. If specified, this query will
        only return rows where the columns meet all the conditions. The columns specified
        in this dictionary must be exactly the unique columns that versioning pivots around.
    """
    with session.begin_nested():
        archive_conds_list = _get_conditions_list(table, conds)
        session.execute(
            sa.delete(table.ArchiveTable, whereclause=_get_conditions(archive_conds_list))
        )
        conds_list = _get_conditions_list(table, conds, archive=False)
        session.execute(
            sa.delete(table, whereclause=_get_conditions(conds_list))
        )


def get(
    table,
    session,
    version_id=None,
    t1=None,
    t2=None,
    fields=None,
    conds=None,
    include_deleted=True,
    page=1,
    page_size=100,
):
    """
    :param table: the model class which inherits from
        :class:`~savage.models.user_table.SavageModelMixin` and specifies the model of
        the user table from which we are querying
    :param session: a sqlalchemy session with connections to the database
    :param version_id: if specified, the value of t1 and t2 will be ignored. If specified, this will
        return all records after the specified version_id.
    :param t1: lower bound time for this query; if None or unspecified,
        defaults to the unix epoch. If this is specified and t2 is not, this query
        will simply return the time slice of data at t1. This must either be a valid
        sql time string or a datetime.datetime object.
    :param t2: upper bound time for this query; if both t1 and t2 are none or unspecified,
        this will return the latest data (i.e. time slice of data now). This must either be a
        valid sql time string or a datetime.datetime object.
    :param fields: a list of strings which corresponds to columns in the table; If
        None or unspecified, returns all fields in the table.
    :param conds: a list of dictionary of key value pairs where keys are columns in the table
        and values are values the column should take on. If specified, this query will
        only return rows where the columns meet all the conditions. The columns specified
        in this dictionary must be exactly the unique columns that versioning pivots around.
    :param include_deleted: if ``True``, the response will include deleted changes. Else it will
        only include changes where ``deleted = 0`` i.e. the data was in the user table.
    :param page: the offset of the result set (1-indexed); i.e. if page_size is 100 and page is 2,
        the result set will contain results 100 - 199
    :param page_size: upper bound on number of results to display. Note the actual returned result
        set may be smaller than this due to the roll up.
    """
    limit, offset = _get_limit_and_offset(page, page_size)
    version_col_names = table.version_columns
    if fields is None:
        fields = [name for name in utils.get_column_names(table) if name != 'version_id']

    if version_id is not None:
        return _format_response(utils.result_to_dict(session.execute(
            sa.select([table.ArchiveTable])
            .where(table.ArchiveTable.version_id > version_id)
            .order_by(*_get_order_clause(table.ArchiveTable))
            .limit(page_size)
            .offset(offset)
        )), fields, version_col_names)

    if t1 is None and t2 is None:
        rows = _get_latest_time_slice(table, session, conds, include_deleted, limit, offset)
        return _format_response(rows, fields, version_col_names)

    if t2 is None:  # return a historical time slice
        rows = _get_historical_time_slice(
            table, session, t1, conds, include_deleted, limit, offset
        )
        return _format_response(rows, fields, version_col_names)

    if t1 is None:
        t1 = datetime.utcfromtimestamp(0)

    rows = _get_historical_changes(
        table, session, conds, t1, t2, include_deleted, limit, offset
    )
    return _format_response(rows, fields, version_col_names)


def _format_response(rows, fields, unique_col_names):
    """This function will look at the data column of rows and extract the specified fields. It
    will also dedup changes where the specified fields have not changed. The list of rows should
    be ordered by the compound primary key which versioning pivots around and be in ascending
    version order.

    This function will return a list of dictionaries where each dictionary has the following
    schema:
        {
            'updated_at': timestamp of the change,
            'version': version number for the change,
            'data': a nested dictionary containing all keys specified in fields and values
                corresponding to values in the user table.
        }

    Note that some versions may be omitted in the output for the same key if the specified fields
    were not changed between versions.

    :param rows: a list of dictionaries representing rows from the ArchiveTable.
    :param fields: a list of strings of fields to be extracted from the archived row.
    """
    output = []
    old_id = None
    for row in rows:
        id_ = {k: row[k] for k in unique_col_names}
        formatted = {k: row[k] for k in row if k != 'data'}
        if id_ != old_id:  # new unique versioned row
            data = row['data']
            formatted['data'] = {k: data.get(k) for k in fields}
            output.append(formatted)
        else:
            data = row['data']
            pruned_data = {k: data.get(k) for k in fields}
            if (
                pruned_data != output[-1]['data'] or
                row['deleted'] != output[-1]['deleted']
            ):
                formatted['data'] = pruned_data
                output.append(formatted)
        old_id = id_
    return output


def _get_conditions(pk_conds, and_conds=None):
    """If and_conds = [a1, a2, ..., an] and pk_conds = [[b11, b12, ..., b1m], ... [bk1, ..., bkm]],
    this function will return the mysql condition clause:
        a1 & a2 & ... an & ((b11 and ... b1m) or ... (b11 and ... b1m))

    :param pk_conds: a list of list of primary key constraints returned by _get_conditions_list
    :param and_conds: additional and conditions to be placed on the query
    """
    if and_conds is None:
        and_conds = []

    if len(and_conds) == 0 and len(pk_conds) == 0:
        return sa.and_()

    condition1 = sa.and_(*and_conds)
    condition2 = sa.or_(*[sa.and_(*cond) for cond in pk_conds])
    return sa.and_(condition1, condition2)


def _get_conditions_list(table, conds, archive=True):
    """This function returns a list of list of == conditions on sqlalchemy columns given conds.
    This should be treated as an or of ands.

    :param table: the user table model class which inherits from
        savage.models.SavageModelMixin
    :param conds: a list of dictionaries of key value pairs where keys are column names and
        values are conditions to be placed on the column.
    :param archive: If true, the condition is with columns from the archive table. Else its from
        the user table.
    """
    if conds is None:
        conds = []

    all_conditions = []
    for cond in conds:
        if len(cond) != len(table.version_columns):
            raise ValueError('Conditions must specify all unique constraints.')

        conditions = []
        t = table.ArchiveTable if archive else table

        for col_name, value in cond.iteritems():
            if col_name not in table.version_columns:
                raise ValueError('{} is not one of the unique columns <{}>'.format(
                    col_name, ','.join(table.version_columns)
                ))
            conditions.append(getattr(t, col_name) == value)
        all_conditions.append(conditions)
    return all_conditions


def _get_historical_changes(table, session, conds, t1, t2, include_deleted, limit, offset):
    pk_conditions = _get_conditions_list(table, conds)
    and_clause = _get_conditions(
        pk_conditions,
        [table.ArchiveTable.updated_at >= t1, table.ArchiveTable.updated_at < t2] +
        [] if include_deleted else [table.ArchiveTable.deleted.is_(False)],
    )

    return utils.result_to_dict(session.execute(
        sa.select([table.ArchiveTable])
        .where(and_clause)
        .order_by(*_get_order_clause(table.ArchiveTable))
        .limit(limit)
        .offset(offset)
    ))


def _get_historical_time_slice(table, session, t, conds, include_deleted, limit, offset):
    at = table.ArchiveTable
    vc = table.version_columns
    pk_conditions = _get_conditions_list(table, conds)
    and_clause = _get_conditions(
        pk_conditions,
        [at.updated_at <= t] +
        [] if include_deleted else [table.ArchiveTable.deleted.is_(False)],
    )
    t2 = at.__table__.alias('t2')
    return utils.result_to_dict(session.execute(
        sa.select([at])
        .select_from(at.__table__.join(
            t2,
            sa.and_(
                t2.c.updated_at <= t,
                at.version_id < t2.c.version_id,
                *[getattr(at, c) == getattr(t2.c, c) for c in vc]
            ),
            isouter=True,
        ))
        .where(t2.c.version_id.is_(None) & and_clause)
        .order_by(*_get_order_clause(at))
        .limit(limit)
        .offset(offset)
    ))


def _get_latest_time_slice(table, session, conds, include_deleted, limit, offset):
    and_clause = _get_conditions(
        _get_conditions_list(table, conds, archive=False),
        [] if include_deleted else [table.ArchiveTable.deleted.is_(False)],
    )
    result = session.execute(
        sa.select([table.ArchiveTable]).select_from(
            table.ArchiveTable.__table__.join(
                table,
                sa.and_(
                    table.ArchiveTable.version_id == table.version_id,
                    *[
                        getattr(table.ArchiveTable, col_name) == getattr(table, col_name)
                        for col_name in table.version_columns
                    ]
                )
            )
        )
        .where(and_clause)
        .order_by(*_get_order_clause(table.ArchiveTable))
        .limit(limit)
        .offset(offset)
    )
    return utils.result_to_dict(result)


def _get_limit_and_offset(page, page_size):
    """Returns a 0-indexed offset and limit based on page and page_size for a MySQL query.
    """
    if page < 1:
        raise ValueError('page must be >= 1')
    limit = page_size
    offset = (page - 1) * page_size
    return limit, offset


def _get_order_clause(archive_table):
    """Returns an ascending order clause on the versioned unique constraint as well as the
    version column.
    """
    order_clause = [
        sa.asc(getattr(archive_table, col_name)) for col_name in archive_table._version_col_names
    ]
    order_clause.append(sa.asc(archive_table.version_id))
    return order_clause
