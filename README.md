# ddcDatabases

[![Donate](https://img.shields.io/badge/Donate-PayPal-brightgreen.svg?style=plastic)](https://www.paypal.com/ncp/payment/6G9Z78QHUD4RJ)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPi](https://img.shields.io/pypi/v/ddcDatabases.svg)](https://pypi.python.org/pypi/ddcDatabases)
[![PyPI Downloads](https://static.pepy.tech/badge/ddcDatabases)](https://pepy.tech/projects/ddcDatabases)
[![codecov](https://codecov.io/gh/ddc/ddcDatabases/graph/badge.svg?token=E942EZII4Q)](https://codecov.io/gh/ddc/ddcDatabases)
[![CI/CD Pipeline](https://github.com/ddc/ddcDatabases/actions/workflows/workflow.yml/badge.svg)](https://github.com/ddc/ddcDatabases/actions/workflows/workflow.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=ddc_ddcDatabases&metric=alert_status)](https://sonarcloud.io/dashboard?id=ddc_ddcDatabases)  
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A//actions-badge.atrox.dev/ddc/ddcDatabases/badge?ref=main&label=build&logo=none)](https://actions-badge.atrox.dev/ddc/ddcDatabases/goto?ref=main)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Python](https://img.shields.io/pypi/pyversions/ddcDatabases.svg)](https://www.python.org/downloads)

[![Support me on GitHub](https://img.shields.io/badge/Support_me_on_GitHub-154c79?style=for-the-badge&logo=github)](https://github.com/sponsors/ddc)


A Python library for database connections and ORM queries with support for multiple database engines including SQLite, PostgreSQL, MySQL, MSSQL, Oracle, and MongoDB.

## Table of Contents

- [Installation](#installation)
  - [Basic Installation (SQLite only)](#basic-installation-sqlite-only)
  - [Database-Specific Installations](#database-specific-installations)
- [Features](#features)
  - [Default Session Settings](#default-session-settings)
- [Database Classes](#database-classes)
  - [SQLite](#sqlite)
  - [MSSQL (SQL Server)](#mssql-sql-server)
  - [PostgreSQL](#postgresql)
  - [MySQL](#mysql)
  - [Oracle](#oracle)
  - [MongoDB](#mongodb)
- [Database Engines](#database-engines)
- [Database Utilities](#database-utilities)
  - [Available Methods](#available-methods)
- [Development](#development)
  - [Building from Source](#building-from-source)
  - [Running Tests](#running-tests)
- [License](#license)
- [Support](#support)


## Features

- **Multiple Database Support**: SQLite, PostgreSQL, MySQL, MSSQL, Oracle, and MongoDB
- **Sync and Async Support**: Both synchronous and asynchronous operations
- **Environment Configuration**: Optional parameters with `.env` file fallback
- **SQLAlchemy Integration**: Built on top of SQLAlchemy ORM
- **Connection Pooling**: Configurable connection pooling for better performance

### Default Session Settings

**Synchronous Sessions:**
- `autoflush = True`
- `expire_on_commit = True` 
- `echo = False`

**Asynchronous Sessions:**
- `autoflush = True`
- `expire_on_commit = False`
- `echo = False`

**Note:** All constructor parameters are optional and fall back to [.env](./ddcDatabases/.env.example) file variables.


## Installation

### Basic Installation (SQLite only)
```shell
pip install ddcDatabases
```

**Note:** The basic installation includes only SQlite. Database-specific drivers are optional extras that you can install as needed.

### Database-Specific Installations

Install only the database drivers you need:

```shell
# All database drivers (recommended for development)
pip install ddcDatabases[all]

# SQL Server / MSSQL
pip install ddcDatabases[mssql]

# MySQL / MariaDB
pip install ddcDatabases[mysql]

# PostgreSQL
pip install ddcDatabases[pgsql]

# Oracle Database
pip install ddcDatabases[oracle]

# MongoDB
pip install ddcDatabases[mongodb]

# Multiple databases (example)
pip install ddcDatabases[mysql,pgsql,mongodb]
```

**Available Database Extras:**
- `all` - All database drivers
- `mssql` - Microsoft SQL Server (pyodbc, aioodbc)
- `mysql` - MySQL/MariaDB (pymysql, aiomysql)
- `pgsql` - PostgreSQL (psycopg2-binary, asyncpg)
- `oracle` - Oracle Database (cx-oracle)
- `mongodb` - MongoDB (pymongo)

**Platform Notes:**
- SQLite support is included by default (no extra installation required)
- PostgreSQL extras may have compilation requirements on some systems
- All extras support both synchronous and asynchronous operations where applicable


## Database Classes

### SQLite

```python
class Sqlite(
    filepath: Optional[str] = None,
    echo: Optional[bool] = None,
    autoflush: Optional[bool] = None,
    expire_on_commit: Optional[bool] = None,
    extra_engine_args: Optional[dict] = None,
)
```

**Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, Sqlite
from your_models import User  # Your SQLAlchemy model

with Sqlite() as session:
    db_utils = DBUtils(session)
    stmt = sa.select(User).where(User.id == 1)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```





### MSSQL (SQL Server)

```python
class MSSQL(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    echo: Optional[bool] = None,
    pool_size: Optional[int] = None,
    max_overflow: Optional[int] = None,
    autoflush: Optional[bool] = None,
    expire_on_commit: Optional[bool] = None,
    extra_engine_args: Optional[dict] = None,
)
```

**Synchronous Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MSSQL
from your_models import User

with MSSQL() as session:
    stmt = sa.select(User).where(User.id == 1)
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

**Asynchronous Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtilsAsync, MSSQL
from your_models import User

async def main():
    async with MSSQL() as session:
        stmt = sa.select(User).where(User.id == 1)
        db_utils = DBUtilsAsync(session)
        results = await db_utils.fetchall(stmt)
        for row in results:
            print(row)
```






### PostgreSQL

```python
class PostgreSQL(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    echo: Optional[bool] = None,
    autoflush: Optional[bool] = None,
    expire_on_commit: Optional[bool] = None,
    engine_args: Optional[dict] = None,
)
```

**Synchronous Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, PostgreSQL
from your_models import User

with PostgreSQL() as session:
    stmt = sa.select(User).where(User.id == 1)
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

**Asynchronous Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtilsAsync, PostgreSQL
from your_models import User

async def main():
    async with PostgreSQL() as session:
        stmt = sa.select(User).where(User.id == 1)
        db_utils = DBUtilsAsync(session)
        results = await db_utils.fetchall(stmt)
        for row in results:
            print(row)
```

### MySQL

**Synchronous Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MySQL

with MySQL() as session:
    stmt = sa.text("SELECT * FROM users WHERE id = :user_id")
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt, {"user_id": 1})
    for row in results:
        print(row)
```




### Oracle

```python
class Oracle(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    servicename: Optional[str] = None,
    echo: Optional[bool] = None,
    autoflush: Optional[bool] = None,
    expire_on_commit: Optional[bool] = None,
    extra_engine_args: Optional[dict] = None,
)
```

**Example with explicit credentials:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, Oracle

credentials = {
    "host": "127.0.0.1",
    "user": "system",
    "password": "oracle",
    "servicename": "xe",
    "echo": False,
}

with Oracle(**credentials) as session:
    stmt = sa.text("SELECT * FROM dual")
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```








### MongoDB

```python
class MongoDB(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    batch_size: Optional[int] = None,
    limit: Optional[int] = None,
)
```

**Example with explicit credentials:**
```python
from ddcDatabases.mongodb import MongoDB
from bson.objectid import ObjectId

credentials = {
    "host": "127.0.0.1",
    "user": "admin",
    "password": "admin",
    "database": "admin",
}

with MongoDB(**credentials) as mongodb:
    query = {"_id": ObjectId("6772cf60f27e7e068e9d8985")}
    collection = "movies"
    with mongodb.cursor(collection, query) as cursor:
        for document in cursor:
            print(document)
```








## Database Engines

Access the underlying SQLAlchemy engine for advanced operations:

**Synchronous Engine:**
```python
from ddcDatabases import PostgreSQL

with PostgreSQL() as session:
    engine = session.bind
    # Use engine for advanced operations
```

**Asynchronous Engine:**
```python
from ddcDatabases import PostgreSQL

async def main():
    async with PostgreSQL() as session:
        engine = session.bind
        # Use engine for advanced operations
```




## Database Utilities

The `DBUtils` and `DBUtilsAsync` classes provide convenient methods for common database operations:

### Available Methods

```python
from ddcDatabases import DBUtils, DBUtilsAsync

# Synchronous utilities
db_utils = DBUtils(session)
results = db_utils.fetchall(stmt)           # Returns list of RowMapping objects
value = db_utils.fetchvalue(stmt)           # Returns single value as string
db_utils.insert(stmt)                       # Insert into model table
db_utils.deleteall(model)                   # Delete all records from model
db_utils.insertbulk(model, data_list)      # Bulk insert from list of dictionaries
db_utils.execute(stmt)                      # Execute any SQLAlchemy statement

# Asynchronous utilities (similar interface with await)
db_utils_async = DBUtilsAsync(session)
results = await db_utils_async.fetchall(stmt)
```




## Development

### Building from Source
```shell
poetry build -f wheel
```

### Running Tests
```shell
poetry update --with test
poe tests
```

## License

Released under the [MIT License](LICENSE)

## Support

If you find this project helpful, consider supporting development:

- [GitHub Sponsor](https://github.com/sponsors/ddc)
- [ko-fi](https://ko-fi.com/ddcsta)
- [PayPal](https://www.paypal.com/ncp/payment/6G9Z78QHUD4RJ)
