# savage
[![Build Status](https://travis-ci.org/NerdWalletOSS/savage.svg?branch=master)](https://travis-ci.org/NerdWalletOSS/savage)
![PyPI](https://img.shields.io/pypi/v/savage.svg)

A library built on top of the SQLAlchemy ORM for versioning row changes to PostgreSQL tables.

Based on [versionalchemy](https://github.com/NerdWalletOSS/versionalchemy)

Author: [Jeremy Lewis](https://www.github.com/luislew/)

## Why not use versionalchemy?

`versionalchemy` executes four SQL statements for every versioned row that is inserted/updated/deleted:

  1. `INSERT`|`UPDATE`|`DELETE`: Insert/update/delete of the versioned row

  2. `SELECT max(va_version) ...`: Selects the current max `va_version` from archive table based on row

  3. `INSERT ...`: Inserts a new row into the archive table, with `va_version` incremented from previous result

  4. `UPDATE ... SET va_id = ...`: Update versioned row with `va_id`, returned after the previous result executes

PostgreSQL has a couple of features that allow for a simpler implementation:

  * `RETURNING`: PostgreSQL allows you to return server generated column values on `INSERT`/`UPDATE`

  * `txid_current()`: System function that returns a monotonically increasing 64-bit int ID for current transaction

Utilizing these two features allows for a much simpler implementation. Instead of storing `va_id` on the archived
table, we store `version_id` (generated server-side using `txid_current()`) on both the archived and archive tables.
As a result, we don't need to select the max version (b/c it's handled server-side), and we don't need to update
the archive row with `archive_id`.

## Getting Started

Sample Usage

```python
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import UniqueConstraint

from savage import init
from savage.models import SavageLogMixin, SavageModelMixin

POSTGRESQL_URL = '<insert postgresql url here>'
engine = create_engine(POSTGRESQL_URL)
Base = declarative_base(bind=engine)


class Example(Base, SavageModelMixin):
    __tablename__ = 'example'
    version_columns = ['id']
    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.String(128))


class ExampleArchive(Base, SavageLogMixin):
    __tablename__ = 'example_archive'
    __table_args__ = (
        UniqueConstraint('id', 'version_id'),
    )
    id = sa.Column(sa.Integer)
    user_id = sa.Column(sa.Integer)


init()  # Only call this once
Example.register(ExampleArchive, engine)  # Call this once per engine, AFTER init()
```

## Latency

We compared the results of [benchmark.py](https://gist.github.com/akshaynanavati/f1e816596d100a33e4b4a9c48099a8b7) to
a comparable [benchmark.py](https://github.com/NerdWalletOSS/savage/blob/master/benchmark.py) written for Savage. It times the performance of inserts using SQLAlchemy core, ORM
with and without version tracking, and (for Savage only) bulk inserts with versioning.

The below stats were generated for 100,000 records using local Docker containers with MySQL and Postgres (average of 3 runs).

|        | Core Inserts | ORM Inserts | Versioned ORM | Bulk Versioning
|--------|:------------:|:-----------:|:-------------:|:---------------:
| VersionAlchemy/MySQL 5.6 | 135 s. | 203 s. | 489 s. | _unsupported_
| Savage/Postgres 9.6 | 154 s. (**-12%**) | 177 s. (**+15%**) | 283 s. (**+73%**) | 17.7 s. (**+2,658%**)

* VersionAlchemy: ~5 ms./record
* Savage: ~3 ms./record
* Bulk insert/archive with Savage: ~180 µs./record (!!)


## Caveats

`txid_current()` depends on executing within a single transaction context.

```python
from models import db, Example

example = Example(value='foo')
with db.session.begin():
    db.session.add(example)
    db.session.commit()

    example.value = 'bar'
    db.session.add(example)
    db.session.commit()  # This will raise an IntegrityError because `txid_current()` hasn't changed
```

Note that this is only an issue if you try to commit the same archived row multiple times within a single transaction.

The following would work just fine:

```python
from models import db, Example

example = Example(value='foo')
db.session.add(example)
db.session.commit()

example.value = 'bar'
db.session.add(example)
db.session.commit()
```

## Why is it called Savage?

**S**QL**A**lchemy**V**ersion**A**lchemyPost**g**r**e**s

## Style

- Follow PEP8 with a line length of 100 characters
- Prefer parenthesis to `\` for line breaks

## License

[MIT License](https://github.com/NerdWalletOSS/savage/blob/master/LICENSE)
