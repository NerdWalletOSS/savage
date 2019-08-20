Changelog
=========

# 1.0.0

First major version release! Major changes:
* Added support for Python 3.6+
* Switched to using virtualenv + pip-tools for dependency management
* Now uses `black` for auto-formatting

# 0.2.3

* Smarter patching of bind processors for JSON/JSONB column types:
  * Skip bind processing completely for bare JSON/JSONB types
  * Only run custom bind processing for decorated JSON/JSONB types
  * Fall back to deserializing for all other JSON/JSONB types

This avoids the unnecessary overhead of JSON dump/load when possible.

# 0.2.2

* Broaden test for JSON column type to handle custom JSON types:
   * Updated check to see if compiled type is `'JSON'` or `'JSONB'`
   * Added `JSONWrapper` column type to validate broader check
   * Fixed PG port in `tests.db_utils`
* Fix CI check in get_pg_config()
* Fix CI_PG_CONFIG

# 0.2.1

* Don't serialize JSON columns as strings when archiving

# 0.2.0

* Add a GIN index on `SavageLogMixin.data`
* Use server-side timestamps for `SavageLogMixin.updated_at`

NOTE: Both of the above require Alembic migrations after upgrading from older versions of Savage
