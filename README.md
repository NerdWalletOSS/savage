# savage
[![Build Status](https://travis-ci.org/NerdWalletOSS/savage.svg?branch=master)](https://travis-ci.org/NerdWalletOSS/savage)
[![PyPI](https://img.shields.io/pypi/v/savage.svg)](https://pypi.org/project/savage/)

A library built on top of the SQLAlchemy ORM for versioning row changes to PostgreSQL tables.

Based on [versionalchemy](https://github.com/NerdWalletOSS/versionalchemy)

Author: [Jeremy Lewis](https://www.github.com/luislew/)

## Why not use versionalchemy?

`versionalchemy` executes four SQL statements for every versioned row that is inserted/updated/deleted:

  1. `INSERT`|`UPDATE`|`DELETE`: Insert/update/delete of the versioned row

  2. `SELECT max(version_id) ...`: Selects the current max `version_id` from archive table based on row

  3. `INSERT ...`: Inserts a new row into the archive table, with `version_id` incremented from previous result

  4. `UPDATE ... SET archive_id = ...`: Update versioned row with `archive_id`, returned after the previous result executes

PostgreSQL has a couple of features that allow for a simpler implementation:

  * `RETURNING`: PostgreSQL allows you to return server generated column values on `INSERT`/`UPDATE`

  * `txid_current()`: System function that returns a monotonically increasing 64-bit int ID for current transaction

Utilizing these two features allows for a much simpler implementation. Instead of storing `archive_id` on the archived
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

Model methods
----------------

Assume we create a new model:

.. code-block:: python

    item = Example(value='initial') 
    item._updated_by = 'user_id_1'  # you can use integer user identifier here from your authorized user model, for versionalchemey it is just a tag
    session.add(item)
    session.commit()  


This will add first version in **example_archive** table and sets **archive_id** on instance, e.g.

.. code-block:: python

    item = session.query(Example).get(item.id)
    print(item.archive_id)  # 123


Now we can use **version_list** to show all versions:

.. code-block:: python

    print(item.version_list(session))
    # [
    #		{'archive_id': 123, 'user_id': 'user_id_1', version_id: 0},
    # ]


Let's change value:

.. code-block:: python

    item.val = 'changed'
    item._updated_by = 'user_id_2'
    session.commit()
    print(item.version_list(session))
    # [
    #       {'archive_id': 123, 'user_id': 'user_id_1', 'version_id': 0},
    #       {'archive_id': 124, 'user_id': 'user_id_2', 'version_id': 1},
    # ]

You can get specific version of model using **version_get**:

.. code-block:: python

    item.version_get(session, archive_id=123)
    # {
    #  'archive_id': 123, 
    #  'id': 1, 
    #  'value': 'initial'    
    # }

You can pass `version_id` instead of `archive_id`:

.. code-block:: python

    item.version_get(session, version_id=0)
    item.version_get(session, 0) # or even
    # both return same as code snippet above


You can also get all revisions:

.. code-block:: python

    item.version_get_all(session)
    # [
    #   {
    #     'archive_id': 123, 
    #     'id': 1,
    #     'record': {
    #       'value': 'initial'
    #     },
    #     'user_id': 'user_id_1',
    #     'version_id': 0
    #   },
    #   {
    #     'archive_id': 124, 
    #     'id': 1,
    #     'record': {
    #       'value': 'changed'
    #     },
    #     'user_id': 'user_id_2',
    #     'version_id': 1
    #   }
    # ]


To check difference betweeen current and previous versions use **version_diff**:

.. code-block:: python

    item.version_diff(session, archive_id=124) # or item.version_diff(session, version_id=0)
    # {
    #   'version_prev_version': 1,
    #   'version_id': 2,
    #   'prev_user_id': 'user_id_1',
    #   'user_id': 'user_id_2',
    #   'change': {
    #     'value': {
    #       'prev': 'initial',
    #       'this': 'changed'
    #     }
    #   }
    # }


**version_diff_all** will show you diffs between all versions:


.. code-block:: python

    item.version_diff_all(session)
    # [
    #   {
    #     'version_prev_version': 0,
    #     'version_id': 1,
    #     'prev_user_id': None,
    #     'user_id': 'user_id_1',
    #     'change': {
    #       'value': {
    #         'prev': None,
    #         'this': 'initial'
    #       }
    #     }
    #   },
    #   {
    #     'version_prev_version': 1,
    #     'version_id': 2,
    #     'prev_user_id': 'user_id_1',
    #     'user_id': 'user_id_2',
    #     'change': {
    #       'value': {
    #         'prev': 'initial',
    #         'this': 'changed'
    #       }
    #     }
    #   },
    # ]



You can restore some previous version using **version_restore**:

.. code-block:: python

    item.version_restore(session, archive_id=123)  # or item.version_restore(session, version_id=0)
    item = session.query(Example).get(item.id)
    print(item.value)  # initial
    

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
