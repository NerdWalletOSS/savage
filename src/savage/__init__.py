"""
SQLAlchemy events integration to automatically archive data on archived model changes.

# Basic strategy

For all archived models, store a version ID which is set by default on insert
and updated when the model is updated. We use the PG function `txid_current()`,
which is guaranteed to be monotonically increasing. We rely on archiving data after flush
because server side generated values (like `id`/`version_id`) are only populated at that point.

*Before flush*: Check all dirty records and update row version ID if the record was modified.
*After flush*: Archive all new/deleted records, and any dirty records where version ID changed.

## Special note on deleted records

Because there is no new row version ID generated during a deletion, the archive row can't set its
version ID to the row version ID without leading to a DB integrity error. Instead, archive rows
for deleted data use `txid_current()` for version ID (see `SavageLogMixin.build_row_dict`).
"""
from sqlalchemy import event, insert, inspect
from sqlalchemy.orm import Session

from savage.exceptions import LogTableCreationError
from savage.models import SavageModelMixin
from savage.utils import get_column_attribute, get_dialect, is_modified

_initialized = False


def init():
    global _initialized

    if _initialized:
        return
    _initialized = True

    event.listen(Session, 'before_flush', _before_flush_handler)
    event.listen(Session, 'after_flush', _after_flush_handler)


def is_initialized():
    global _initialized
    return _initialized


def _before_flush_handler(session, _flush_context, _instances):
    """Update version ID for all dirty, modified rows"""
    dialect = get_dialect(session)
    for row in session.dirty:
        if isinstance(row, SavageModelMixin) and is_modified(row, dialect):
            # Update row version_id
            row.update_version_id()


def _after_flush_handler(session, _flush_context):
    """Archive all new/updated/deleted data"""
    dialect = get_dialect(session)
    handlers = [
        (_versioned_delete, session.deleted),
        (_versioned_insert, session.new),
        (_versioned_update, session.dirty),
    ]
    for handler, rows in handlers:
        # TODO: Bulk archive insert statements
        for row in rows:
            if not isinstance(row, SavageModelMixin):
                continue
            if not hasattr(row, 'ArchiveTable'):
                raise LogTableCreationError('Need to register Savage tables!!')
            user_id = getattr(row, '_updated_by', None)
            handler(row, session, user_id, dialect)


def _versioned_delete(row, *args):
    _archive_row(row, *args, deleted=True)


def _versioned_insert(row, *args):
    _archive_row(row, *args)


def _versioned_update(row, *args):
    # Do nothing if version_id is unchanged
    previous_version_id = get_column_attribute(row, 'version_id', use_dirty=False)
    if previous_version_id == row.version_id:
        return

    # Check if composite key has been changed
    row_attrs = inspect(row).attrs
    composite_key_changed = any(
        getattr(row_attrs, col).history.has_changes() for col in row.version_columns
    )
    if composite_key_changed:
        # Add deleted archive entry for pre-changed state, but with current version_id
        _archive_row(row, *args, deleted=True, use_dirty=False)

    _archive_row(row, *args)


def _archive_row(row, session, user_id, dialect, **kwargs):
    archive_table = row.ArchiveTable
    archive_row_dict = archive_table.build_row_dict(row, dialect, user_id=user_id, **kwargs)
    session.execute(insert(archive_table), archive_row_dict)
