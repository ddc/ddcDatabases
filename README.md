<div class="shields">
  <h1>ddcDatabases</h1>
</div>

<div class="shields">
    <a href="https://www.paypal.com/ncp/payment/6G9Z78QHUD4RJ">
        <img src="https://img.shields.io/badge/Donate-PayPal-brightgreen.svg?style=plastic" alt="Donate"/>
    </a>
  <a href="https://github.com/sponsors/ddc">
    <img src="https://img.shields.io/static/v1?label=Sponsor&message=%E2%9D%A4&logo=GitHub&color=ff69b4" alt="Sponsor"/>
  </a>
</div>

<div class="shields">
    <a href="https://www.python.org/downloads">
        <img src="https://img.shields.io/pypi/pyversions/ddcDatabases.svg" alt="Python"/>
    </a>
    <a href="https://opensource.org/licenses/MIT">
        <img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"/>
    </a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Code style: black"/>
    </a>
    <a href="https://pepy.tech/projects/ddcDatabases">
        <img src="https://static.pepy.tech/badge/ddcDatabases" alt="PyPI Downloads"/>
    </a>
    <a href="https://pypi.python.org/pypi/ddcDatabases">
        <img src="https://img.shields.io/pypi/v/ddcDatabases.svg" alt="PyPi"/>
    </a>
</div>

<div class="shields">
    <a href="https://github.com/ddc/ddcDatabases/issues">
        <img src="https://img.shields.io/github/issues/ddc/ddcDatabases" alt="issues"/>
    </a>    
    <a href="https://snyk.io/test/github/ddc/ddcDatabases">
        <img src="https://snyk.io/test/github/ddc/ddcDatabases/badge.svg" alt="Known Vulnerabilities"/>
    </a>   
    <a href="https://codecov.io/gh/ddc/ddcDatabases">
        <img src="https://codecov.io/gh/ddc/ddcDatabases/graph/badge.svg?token=XWB53034GI" alt="codecov"/>
    </a>
    <a href="https://sonarcloud.io/dashboard?id=ddc_ddcDatabases">
        <img src="https://sonarcloud.io/api/project_badges/measure?project=ddc_ddcDatabases&metric=alert_status" alt="Quality Gate Status"/>
    </a>
     <a href="https://github.com/ddc/ddcDatabases/actions/workflows/workflow.yml">
        <img src="https://github.com/ddc/ddcDatabases/actions/workflows/workflow.yml/badge.svg" alt="CI/CD Pipeline"/>
    </a>
     <a href="https://actions-badge.atrox.dev/ddc/ddcDatabases/goto?ref=main">
        <img src="https://img.shields.io/endpoint.svg?url=https%3A//actions-badge.atrox.dev/ddc/ddcDatabases/badge?ref=main&label=build&logo=none" alt="Build Status"/>
    </a>
</div>

<div class="shields">
  <p>A Python library for database connections and ORM queries with support for multiple database engines including SQLite, PostgreSQL, MySQL, MSSQL, Oracle, and MongoDB.</p>
</div>

<style>
.shields {
    justify-content: center;
    text-align:center;
}
</style>


## Table of Contents

- [Installation](#installation)
  - [Basic Installation (SQLite only)](#basic-installation-sqlite-only)
  - [Database-Specific Installations](#database-specific-installations)
- [Features](#features)
  - [Default Session Settings](#default-session-settings)
  - [Retry Logic](#retry-logic)
  - [Persistent Connections](#persistent-connections)
- [Database Classes](#database-classes)
  - [SQLite](#sqlite)
  - [MSSQL (SQL Server)](#mssql-sql-server)
  - [PostgreSQL](#postgresql)
  - [MySQL/MariaDB](#mysqlmariadb)
  - [Oracle](#oracle)
  - [MongoDB](#mongodb)
- [Database Engines](#database-engines)
- [Database Utilities](#database-utilities)
  - [Available Methods](#available-methods)
- [Logging](#logging)
- [Development](#development)
  - [Building from Source](#building-from-source)
  - [Running Tests](#running-tests)
- [License](#license)
- [Support](#support)


## Features

- **Multiple Database Support**: SQLite, PostgreSQL, MySQL/MariaDB, MSSQL, Oracle, and MongoDB
- **Sync and Async Support**: Both synchronous and asynchronous operations
- **Environment Configuration**: Optional parameters with `.env` file fallback
- **SQLAlchemy Integration**: Built on top of SQLAlchemy ORM
- **Connection Pooling**: Configurable connection pooling for better performance
- **Retry Logic**: Automatic retry with exponential backoff for connection errors
- **Persistent Connections**: Singleton connection managers with idle timeout and auto-reconnection

### Default Session Settings

- `autoflush = False`
- `expire_on_commit = False` 
- `echo = False`

**Note:** All constructor parameters are optional and fall back to [.env](./ddcDatabases/.env.example) file variables.


### Retry Logic

Retry with exponential backoff is enabled by default at two levels:

**1. Connection Level** - Retries when establishing database connections:
```python
from ddcDatabases import PostgreSQL

with PostgreSQL(
    enable_retry=True,           # Enable/disable retry (default: True)
    max_retries=3,               # Maximum retry attempts (default: 3)
    initial_retry_delay=1.0,     # Initial delay in seconds (default: 1.0)
    max_retry_delay=30.0,        # Maximum delay in seconds (default: 30.0)
) as session:
    # Connection errors will automatically retry with exponential backoff
    pass
```

**2. Operation Level** - Retries individual database operations (fetchall, insert, etc.):
```python
from ddcDatabases import DBUtils, RetryConfig, PostgreSQL

with PostgreSQL() as session:
    # Custom retry config for operations
    retry_config = RetryConfig(
        enable_retry=True,       # Enable/disable (default: True)
        max_retries=3,           # Max attempts (default: 3)
        initial_delay=1.0,       # Initial delay in seconds (default: 1.0)
        max_delay=30.0,          # Max delay in seconds (default: 30.0)
        jitter=0.1,              # Randomization factor (default: 0.1)
    )
    db_utils = DBUtils(session, retry_config=retry_config)

    # Operations will retry on connection errors
    results = db_utils.fetchall(stmt)
```

**Retry Settings by Database:**
| Database   | `enable_retry` | `max_retries` |
|------------|----------------|---------------|
| PostgreSQL | `True`         | `3`           |
| MySQL      | `True`         | `3`           |
| MSSQL      | `True`         | `3`           |
| Oracle     | `True`         | `3`           |
| MongoDB    | `True`         | `3`           |
| SQLite     | `False`        | `1`           |


### Persistent Connections

For long-running applications, use persistent connections with automatic reconnection and idle timeout:

```python
from ddcDatabases import (
    PostgreSQLPersistent,
    MySQLPersistent,
    MongoDBPersistent,
    close_all_persistent_connections,
)

# Get or create a persistent connection (singleton per connection params)
conn = PostgreSQLPersistent(
    host="localhost",
    user="postgres",
    password="postgres",
    database="mydb",
)

# Use as context manager (doesn't disconnect on exit, just updates last-used time)
with conn as session:
    # Use session...
    pass

# Connection stays alive and will auto-reconnect if needed
# Idle connections are automatically closed after timeout (default: 300s)

# For async connections
conn = PostgreSQLPersistent(async_mode=True)
async with conn as session:
    # Use async session...
    pass

# Cleanup all persistent connections on application shutdown
close_all_persistent_connections()
```

**Available Persistent Connection Classes:**
- `PostgreSQLPersistent` - PostgreSQL (sync/async)
- `MySQLPersistent` - MySQL/MariaDB (sync/async)
- `MSSQLPersistent` - MSSQL (sync/async)
- `OraclePersistent` - Oracle (sync only)
- `MongoDBPersistent` - MongoDB (sync only)


## Installation

### Basic Installation (SQLite only)
```shell
pip install ddcDatabases
```

**Note:** The basic installation includes only SQlite. Database-specific drivers are optional extras that you can install as needed.

### Database-Specific Installations

Install only the database drivers you need:

```shell
# All database drivers
pip install "ddcDatabases[all]"

# SQL Server / MSSQL
pip install "ddcDatabases[mssql]"

# MySQL/MariaDB
pip install "ddcDatabases[mysql]"

# PostgreSQL
pip install "ddcDatabases[pgsql]"

# Oracle Database
pip install "ddcDatabases[oracle]"

# MongoDB
pip install "ddcDatabases[mongodb]"

# Multiple databases (example)
pip install "ddcDatabases[mysql,pgsql,mongodb]"
```

**Available Database Extras:**
- `all` - All database drivers
- `mssql` - Microsoft SQL Server (pyodbc, aioodbc)
- `mysql` - MySQL and MariaDB (pymysql, aiomysql)
- `pgsql` - PostgreSQL (psycopg2-binary, asyncpg)
- `oracle` - Oracle Database (cx-oracle)
- `mongodb` - MongoDB (pymongo)

**Platform Notes:**
- SQLite support is included by default (no extra installation required)
- PostgreSQL extras may have compilation requirements on some systems
- All extras support both synchronous and asynchronous operations where applicable


## Database Classes

### SQLite

**Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, Sqlite
from your_models import Model  # Your SQLAlchemy model

with Sqlite(filepath="data.db") as session:
    db_utils = DBUtils(session)
    stmt = sa.select(Model).where(Model.id == 1)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```





### MSSQL (SQL Server)

**Synchronous Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MSSQL
from your_models import Model

kwargs = {
    "host": "127.0.0.1",
    "port": 1433,
    "user": "sa",
    "password": "password",
    "database": "master",
    "db_schema": "dbo",
    "echo": True,
    "autoflush": True,
    "expire_on_commit": True,
    "autocommit": True,
    "connection_timeout": 30,
    "pool_recycle": 3600,
    "pool_size": 25,
    "max_overflow": 50,
}

with MSSQL(**kwargs) as session:
    stmt = sa.select(Model).where(Model.id == 1)
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

**Asynchronous Example:**
```python
import asyncio
import sqlalchemy as sa
from ddcDatabases import DBUtilsAsync, MSSQL
from your_models import Model

async def main():
    async with MSSQL(**kwargs) as session:
        stmt = sa.select(Model).where(Model.id == 1)
        db_utils = DBUtilsAsync(session)
        results = await db_utils.fetchall(stmt)
        for row in results:
            print(row)
asyncio.run(main())
```






### PostgreSQL

**Synchronous Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, PostgreSQL
from your_models import Model

kwargs = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "database": "postgres",
    "echo": True,
    "autoflush": False,
    "expire_on_commit": False,
    "autocommit": True,
    "connection_timeout": 30,
    "pool_recycle": 3600,
    "pool_size": 25,
    "max_overflow": 50,
}

with PostgreSQL(**kwargs) as session:
    stmt = sa.select(Model).where(Model.id == 1)
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

**Asynchronous Example:**
```python
import asyncio
import sqlalchemy as sa
from ddcDatabases import DBUtilsAsync, PostgreSQL
from your_models import Model

async def main():
    async with PostgreSQL(**kwargs) as session:
        stmt = sa.select(Model).where(Model.id == 1)
        db_utils = DBUtilsAsync(session)
        results = await db_utils.fetchall(stmt)
        for row in results:
            print(row)
asyncio.run(main())
```







### MySQL/MariaDB

The MySQL class is fully compatible with both MySQL and MariaDB databases.

**Synchronous Example:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MySQL

kwargs = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "dev",
    "echo": True,
    "autoflush": False,
    "expire_on_commit": False,
    "autocommit": True,
    "connection_timeout": 30,
    "pool_recycle": 3600,
    "pool_size": 25,
    "max_overflow": 50,
}

with MySQL(**kwargs) as session:
    stmt = sa.text("SELECT * FROM users WHERE id = 1")
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

**Asynchronous Example:**
```python
import asyncio
import sqlalchemy as sa
from ddcDatabases import DBUtilsAsync, MySQL
async def main() -> None:
    async with MySQL(**kwargs) as session:
        stmt = sa.text("SELECT * FROM users")
        db_utils = DBUtilsAsync(session)
        results = await db_utils.fetchall(stmt)
        for row in results:
            print(row)
asyncio.run(main())

```





### Oracle

**Example with explicit credentials:**
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, Oracle

kwargs = {
    "host": "127.0.0.1",
    "port": 1521,
    "user": "system",
    "password": "oracle",
    "servicename": "xe",
    "echo": False,
    "autoflush": False,
    "expire_on_commit": False,
    "autocommit": True,
    "connection_timeout": 30,
    "pool_recycle": 3600,
    "pool_size": 25,
    "max_overflow": 50,
}

with Oracle(**kwargs) as session:
    stmt = sa.text("SELECT * FROM dual")
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```








### MongoDB

**Example with explicit credentials:**
```python
from ddcDatabases import MongoDB
from bson.objectid import ObjectId

kwargs = {
    "host": "127.0.0.1",
    "port": 27017,
    "user": "admin",
    "password": "admin",
    "database": "admin",
    "collection": "test_collection",
    "sort_column": "_id",
    "sort_order": "asc", # asc or desc
}

query = {"_id": ObjectId("689c9f71dd642a68cfc60477")}
with MongoDB(**kwargs, query=query) as cursor:
    for each in cursor:
        print(each)
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

The `DBUtils` and `DBUtilsAsync` classes provide convenient methods for common database operations with built-in retry support:

### Available Methods

```python
from ddcDatabases import DBUtils, DBUtilsAsync, RetryConfig

# Synchronous utilities (retry enabled by default)
db_utils = DBUtils(session)
results = db_utils.fetchall(stmt)           # Returns list of RowMapping objects
results = db_utils.fetchall(stmt, as_dict=True)  # Returns list of dictionaries
value = db_utils.fetchvalue(stmt)           # Returns single value as string
db_utils.insert(model_instance)             # Insert into model table
db_utils.deleteall(Model)                   # Delete all records from model
db_utils.insertbulk(Model, data_list)       # Bulk insert from list of dictionaries
db_utils.execute(stmt)                      # Execute any SQLAlchemy statement

# With custom retry configuration
db_utils = DBUtils(session, retry_config=RetryConfig(max_retries=5))

# Disable retry for specific operations
db_utils = DBUtils(session, retry_config=RetryConfig(enable_retry=False))

# Asynchronous utilities (similar interface with await)
db_utils_async = DBUtilsAsync(session)
results = await db_utils_async.fetchall(stmt)
```




## Logging
```python
import logging
logging.getLogger('ddcDatabases').setLevel(logging.INFO)
logging.getLogger('ddcDatabases').addHandler(logging.StreamHandler())
```




## Development
Must have UV installed. See [UV Installation Guide](https://uv.run/docs/getting-started/installation)

### Building from Source
```shell
poe build
```

### Building DEV Environment and Running Tests
```shell
uv venv
poe install
poe test
```




## License

Released under the [MIT License](LICENSE)

## Support

If you find this project helpful, consider supporting development:

- [GitHub Sponsor](https://github.com/sponsors/ddc)
- [ko-fi](https://ko-fi.com/ddcsta)
- [PayPal](https://www.paypal.com/ncp/payment/6G9Z78QHUD4RJ)
