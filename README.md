<h1 align="center">
  <img src="https://raw.githubusercontent.com/ddc/ddcDatabases/main/assets/database-icon.svg" alt="ddcDatabases" width="150">
  <br>
  ddcDatabases
</h1>

<p align="center">
    <a href="https://github.com/sponsors/ddc"><img src="https://img.shields.io/static/v1?style=plastic&label=Sponsor&message=%E2%9D%A4&logo=GitHub&color=ff69b4" alt="Sponsor"/></a>
    <br>
    <a href="https://ko-fi.com/ddcsta"><img src="https://img.shields.io/badge/Ko--fi-ddcsta-FF5E5B?style=plastic&logo=kofi&logoColor=white&color=brightgreen" alt="Ko-fi"/></a>
    <a href="https://www.paypal.com/ncp/payment/6G9Z78QHUD4RJ"><img src="https://img.shields.io/badge/Donate-PayPal-brightgreen.svg?style=plastic&logo=paypal&logoColor=white" alt="Donate"/></a>
    <br>
    <a href="https://pypi.python.org/pypi/ddcDatabases"><img src="https://img.shields.io/pypi/v/ddcDatabases.svg?style=plastic&logo=python&cacheSeconds=3600" alt="PyPi"/></a>
    <a href="https://pepy.tech/projects/ddcDatabases"><img src="https://img.shields.io/pepy/dt/ddcDatabases?style=plastic&logo=pypi&logoColor=3776AB" alt="PyPI Downloads"/></a>
    <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg?style=plastic&logo=creativecommons&logoColor=white" alt="License: MIT"/></a>
    <br>
    <a href="https://www.python.org/downloads"><img src="https://img.shields.io/pypi/pyversions/ddcDatabases.svg?style=plastic&logo=python&cacheSeconds=3600" alt="Python"/></a>
    <a href="https://github.com/astral-sh/uv"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json&style=plastic" alt="uv"/></a>
    <a href="https://github.com/astral-sh/ruff"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&style=plastic" alt="Ruff"/></a>
    <br>
    <a href="https://github.com/ddc/ddcDatabases/issues"><img src="https://img.shields.io/github/issues/ddc/ddcDatabases?style=plastic&logo=github&logoColor=white" alt="issues"/></a>
    <a href="https://codecov.io/gh/ddc/ddcDatabases"><img src="https://img.shields.io/codecov/c/github/ddc/ddcDatabases?token=XWB53034GI&style=plastic&logo=codecov" alt="codecov"/></a>
    <a href="https://sonarcloud.io/dashboard?id=ddc_ddcDatabases"><img src="https://img.shields.io/sonar/quality_gate/ddc_ddcDatabases?server=https%3A%2F%2Fsonarcloud.io&style=plastic&logo=sonarqubecloud&logoColor=white" alt="Quality Gate Status"/></a>
    <a href="https://github.com/ddc/ddcDatabases/actions/workflows/workflow.yml"><img src="https://img.shields.io/github/actions/workflow/status/ddc/ddcDatabases/workflow.yml?style=plastic&logo=github&logoColor=white&label=CI%2FCD%20Pipeline" alt="CI/CD Pipeline"/></a>
    <a href="https://actions-badge.atrox.dev/ddc/ddcDatabases/goto?ref=main"><img src="https://img.shields.io/endpoint.svg?url=https%3A//actions-badge.atrox.dev/ddc/ddcDatabases/badge?ref=main&label=build&logo=github&style=plastic" alt="Build Status"/></a>
</p>

<p align="center">A Python library for database connections and ORM queries with support for multiple database engines. <br> Includes SQLite, PostgreSQL, MySQL/MariaDB, MSSQL, Oracle, and MongoDB</p>


# Table of Contents

- [Features](#features)
  - [Default Session Settings](#default-session-settings)
  - [Configuration Classes](#configuration-classes)
  - [Retry Logic](#retry-logic)
  - [Persistent Connections](#persistent-connections)
- [Installation](#installation)
  - [Basic Installation (SQLite only)](#basic-installation-sqlite-only)
  - [Database-Specific Installations](#database-specific-installations)
- [Database Classes](#database-classes)
  - [SQLite](#sqlite)
  - [MSSQL (SQL Server)](#mssql-microsoft-sql-server)
  - [PostgreSQL](#postgresql)
  - [MySQL/MariaDB](#mysqlmariadb)
  - [Oracle](#oracle)
  - [MongoDB](#mongodb)
- [Database Engines](#database-engines)
- [Database Utilities](#database-utilities)
  - [Available Methods](#available-methods)
- [Logging](#logging)
- [Development and Testing](#development-and-testing)
  - [Create DEV Environment and Running Tests](#create-dev-environment-and-running-tests)
  - [Update DEV Environment Packages](#update-dev-environment-packages)
  - [Building Wheel](#building-wheel)
  - [Optionals](#optionals)
- [License](#license)
- [Support](#support)


# Features

- 🗄️ **Multiple Database Support**: SQLite, PostgreSQL, MySQL/MariaDB, MSSQL, Oracle, and MongoDB
- ⚡ **Sync and Async Support**: Both synchronous and asynchronous operations
- 🔧 **Environment Configuration**: Optional parameters with `.env` file fallback
- 🔗 **SQLAlchemy Integration**: Built on top of SQLAlchemy ORM
- 🏊 **Connection Pooling**: Configurable connection pooling for better performance
- 🔁 **Retry Logic**: Automatic retry with exponential backoff for connection errors
- 🔌 **Persistent Connections**: Singleton connection managers with idle timeout and auto-reconnection

## Default Session Settings

- `autoflush = False`
- `expire_on_commit = False`
- `echo = False`

**Autocommit Defaults by Database:**

| Database   | Default | Convention                       |
|------------|---------|----------------------------------|
| PostgreSQL | `False` | Uses transactions by default     |
| MSSQL      | `False` | Uses transactions by default     |
| MySQL      | `True`  | Autocommit ON is MySQL's default |
| Oracle     | `False` | Requires explicit COMMIT         |

**Note:** All constructor parameters are optional and fall back to [.env](./ddcDatabases/.env.example) file variables.


## Configuration Classes

Database classes use structured configuration dataclasses instead of flat keyword arguments:

| Class                        | Purpose                         | Fields                                                                            |
|------------------------------|---------------------------------|-----------------------------------------------------------------------------------|
| `{DB}PoolConfig`             | Connection pool settings        | `pool_size`, `max_overflow`, `pool_recycle`, `connection_timeout`                 |
| `{DB}SessionConfig`          | SQLAlchemy session settings     | `echo`, `autoflush`, `expire_on_commit`, `autocommit`                             |
| `{DB}ConnectionRetryConfig`  | Connection-level retry settings | `enable_retry`, `max_retries`, `initial_retry_delay`, `max_retry_delay`           |
| `{DB}OperationRetryConfig`   | Operation-level retry settings  | `enable_retry`, `max_retries`, `initial_retry_delay`, `max_retry_delay`, `jitter` |
| `PersistentConnectionConfig` | Persistent connection settings  | `idle_timeout`, `health_check_interval`, `auto_reconnect`                         |

**Note:** Replace `{DB}` with the database prefix: `PostgreSQL`, `MySQL`, `MSSQL`, `Oracle`, `MongoDB`, or `Sqlite`.

**Database-specific SSL/TLS configs:**

| Class                 | Database                                                                                           |
|-----------------------|----------------------------------------------------------------------------------------------------|
| `PostgreSQLSSLConfig` | PostgreSQL (`ssl_mode`, `ssl_ca_cert_path`, `ssl_client_cert_path`, `ssl_client_key_path`)         |
| `MySQLSSLConfig`      | MySQL/MariaDB (`ssl_mode`, `ssl_ca_cert_path`, `ssl_client_cert_path`, `ssl_client_key_path`)      |
| `MSSQLSSLConfig`      | MSSQL (`ssl_encrypt`, `ssl_trust_server_certificate`, `ssl_ca_cert_path`)                          |
| `OracleSSLConfig`     | Oracle (`ssl_enabled`, `ssl_wallet_path`)                                                          |
| `MongoDBTLSConfig`    | MongoDB (`tls_enabled`, `tls_ca_cert_path`, `tls_cert_key_path`, `tls_allow_invalid_certificates`) |

**MongoDB-specific config:**

| Class                | Purpose        | Fields                                                      |
|----------------------|----------------|-------------------------------------------------------------|
| `MongoDBQueryConfig` | Query settings | `query`, `sort_column`, `sort_order`, `batch_size`, `limit` |


## Retry Logic

Retry with exponential backoff is enabled by default at two levels:

**1. Connection Level** - Retries when establishing database connections:
```python
from ddcDatabases import PostgreSQL, PostgreSQLConnectionRetryConfig

with PostgreSQL(
    connection_retry_config=PostgreSQLConnectionRetryConfig(
        enable_retry=True,           # Enable/disable retry (default: True)
        max_retries=3,               # Maximum retry attempts (default: 3)
        initial_retry_delay=1.0,     # Initial delay in seconds (default: 1.0)
        max_retry_delay=30.0,        # Maximum delay in seconds (default: 30.0)
    ),
) as session:
    # Connection errors will automatically retry with exponential backoff
    pass
```

**2. Operation Level** - Retries individual database operations (fetchall, insert, etc.):
```python
from ddcDatabases import DBUtils, PostgreSQL, PostgreSQLOperationRetryConfig

with PostgreSQL(
    operation_retry_config=PostgreSQLOperationRetryConfig(
        enable_retry=True,            # Enable/disable (default: True)
        max_retries=3,                # Max attempts (default: 3)
        initial_retry_delay=1.0,      # Initial delay in seconds (default: 1.0)
        max_retry_delay=30.0,         # Max delay in seconds (default: 30.0)
        jitter=0.1,                   # Randomization factor (default: 0.1)
    ),
) as session:
    db_utils = DBUtils(session)
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


## Persistent Connections

For long-running applications, use persistent connections with automatic reconnection and idle timeout:

```python
from ddcDatabases import (
    PostgreSQLPersistent,
    MySQLPersistent,
    MongoDBPersistent,
    PersistentConnectionConfig,
    PostgreSQLConnectionRetryConfig,
    PostgreSQLOperationRetryConfig,
    close_all_persistent_connections,
)

# Get or create a persistent connection (singleton per connection params)
conn = PostgreSQLPersistent(
    host="localhost",
    user="postgres",
    password="postgres",
    database="mydb",
    config=PersistentConnectionConfig(
        idle_timeout=300,            # seconds before idle disconnect (default: 300)
        health_check_interval=30,    # seconds between health checks (default: 30)
        auto_reconnect=True,         # auto-reconnect on failure (default: True)
    ),
    connection_retry_config=PostgreSQLConnectionRetryConfig(
        enable_retry=True,           # enable connection retry (default: True)
        max_retries=5,               # max connection attempts (default: 5)
        initial_retry_delay=1.0,     # initial delay in seconds (default: 1.0)
        max_retry_delay=30.0,        # max delay in seconds (default: 30.0)
    ),
    operation_retry_config=PostgreSQLOperationRetryConfig(
        enable_retry=True,           # enable operation retry (default: True)
        max_retries=3,               # max operation attempts (default: 3)
        initial_retry_delay=0.5,     # initial delay in seconds (default: 0.5)
        max_retry_delay=10.0,        # max delay in seconds (default: 10.0)
        jitter=0.1,                  # randomization factor (default: 0.1)
    ),
)

# Use as context manager (doesn't disconnect on exit, just updates last-used time)
with conn as session:
    # Use session...
    pass

# Connection stays alive and will auto-reconnect if needed
# Idle connections are automatically closed after timeout (default: 300s)

# For async connections
conn = PostgreSQLPersistent(host="localhost", database="mydb", async_mode=True)
async with conn as session:
    # Use async session...
    pass

# Cleanup all persistent connections on application shutdown
close_all_persistent_connections()
```

### Execute with Retry

The `execute_with_retry` method provides automatic session management with retry logic:

**Synchronous:**
```python
from ddcDatabases import PostgreSQLPersistent

db = PostgreSQLPersistent(logger=logger)
result = db.execute_with_retry(
    lambda session: MyDal(session).do_something()
)
```

**Asynchronous:**
```python
from ddcDatabases import PostgreSQLPersistent

db = PostgreSQLPersistent(async_mode=True, logger=logger)
result = await db.execute_with_retry(
    lambda session: MyDal(session).do_something()
)
```

The method automatically:
- Connects (or reuses existing connection)
- Executes the operation with the session
- Commits on success, rolls back on failure
- Retries with exponential backoff if `auto_reconnect` is enabled

**Available Persistent Connection Classes:**

- `PostgreSQLPersistent` - PostgreSQL (sync/async)
- `MySQLPersistent` / `MariaDBPersistent` - MySQL/MariaDB (sync/async)
- `MSSQLPersistent` - MSSQL (sync/async)
- `OraclePersistent` - Oracle (sync only)
- `MongoDBPersistent` - MongoDB (sync only)


# Installation

## Basic Installation (SQLite only)
```shell
pip install ddcDatabases
```

**Note:** The basic installation includes only SQlite. Database-specific drivers are optional extras that you can install as needed.

## Database-Specific Installations

Install only the database drivers you need:

```shell
# All database drivers
pip install "ddcDatabases[all]"

# SQL Server / MSSQL
pip install "ddcDatabases[mssql]"

# MySQL/MariaDB
pip install "ddcDatabases[mysql]"
# or
pip install "ddcDatabases[mariadb]"

# PostgreSQL
pip install "ddcDatabases[postgres]"
# or
pip install "ddcDatabases[pgsql]"

# Oracle Database
pip install "ddcDatabases[oracle]"

# MongoDB
pip install "ddcDatabases[mongodb]"

# Multiple databases (example)
pip install "ddcDatabases[mysql,postgres,mongodb]"
```

**Available Database Extras:**

- `all` - All database drivers
- `mssql` - Microsoft SQL Server (pyodbc, aioodbc)
- `mysql` - MySQL and MariaDB (mysqlclient, aiomysql)
- `mariadb` - Alias for `mysql`
- `postgres` - PostgreSQL (psycopg, asyncpg)
- `pgsql` - Alias for `postgres`
- `oracle` - Oracle Database (oracledb)
- `mongodb` - MongoDB (motor)

**Platform Notes:**

- SQLite support is included by default (no extra installation required)
- PostgreSQL extras may have compilation requirements on some systems
- All extras support both synchronous and asynchronous operations where applicable


# Database Classes

## SQLite

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


## MSSQL (Microsoft SQL Server)

**Synchronous Example:**

```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MSSQL, MSSQLPoolConfig, MSSQLSessionConfig, MSSQLSSLConfig

with MSSQL(
    host="127.0.0.1",
    port=1433,
    user="sa",
    password="password",
    database="master",
    schema="dbo",
    pool_config=MSSQLPoolConfig(
        pool_size=25,
        max_overflow=50,
        pool_recycle=3600,
        connection_timeout=30,
    ),
    session_config=MSSQLSessionConfig(
        echo=True,
        autoflush=True,
        expire_on_commit=True,
        autocommit=True,
    ),
    ssl_config=MSSQLSSLConfig(
        ssl_encrypt=False,
        ssl_trust_server_certificate=True,
    ),
) as session:
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
    async with MSSQL(host="127.0.0.1", database="master") as session:
        stmt = sa.select(Model).where(Model.id == 1)
        db_utils = DBUtilsAsync(session)
        results = await db_utils.fetchall(stmt)
        for row in results:
            print(row)
asyncio.run(main())
```


## PostgreSQL

**Synchronous Example:**

```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, PostgreSQL, PostgreSQLPoolConfig, PostgreSQLSessionConfig, PostgreSQLSSLConfig

with PostgreSQL(
    host="127.0.0.1",
    port=5432,
    user="postgres",
    password="postgres",
    database="postgres",
    schema="public",
    pool_config=PostgreSQLPoolConfig(
        pool_size=25,
        max_overflow=50,
        pool_recycle=3600,
        connection_timeout=30,
    ),
    session_config=PostgreSQLSessionConfig(
        echo=True,
        autoflush=False,
        expire_on_commit=False,
        autocommit=True,
    ),
    ssl_config=PostgreSQLSSLConfig(
        ssl_mode="disable",              # disable, allow, prefer, require, verify-ca, verify-full
        ssl_ca_cert_path=None,           # Path to CA certificate
        ssl_client_cert_path=None,       # Path to client certificate
        ssl_client_key_path=None,        # Path to client key
    ),
) as session:
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
    async with PostgreSQL(host="127.0.0.1", database="postgres") as session:
        stmt = sa.select(Model).where(Model.id == 1)
        db_utils = DBUtilsAsync(session)
        results = await db_utils.fetchall(stmt)
        for row in results:
            print(row)
asyncio.run(main())
```


## MySQL/MariaDB

The MySQL class is fully compatible with both MySQL and MariaDB databases. For convenience, MariaDB aliases are also available:

```python
# Both imports are equivalent
from ddcDatabases import MySQL, MySQLPoolConfig, MySQLSessionConfig
from ddcDatabases import MariaDB, MariaDBPoolConfig, MariaDBSessionConfig  # Aliases
```

**Synchronous Example:**

```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MySQL, MySQLPoolConfig, MySQLSessionConfig, MySQLSSLConfig

with MySQL(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="root",
    database="dev",
    pool_config=MySQLPoolConfig(
        pool_size=25,
        max_overflow=50,
        pool_recycle=3600,
        connection_timeout=30,
    ),
    session_config=MySQLSessionConfig(
        echo=True,
        autoflush=False,
        expire_on_commit=False,
        autocommit=True,
    ),
    ssl_config=MySQLSSLConfig(
        ssl_mode="DISABLED",             # DISABLED, PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY
        ssl_ca_cert_path=None,
        ssl_client_cert_path=None,
        ssl_client_key_path=None,
    ),
) as session:
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
    async with MySQL(host="127.0.0.1", database="dev") as session:
        stmt = sa.text("SELECT * FROM users")
        db_utils = DBUtilsAsync(session)
        results = await db_utils.fetchall(stmt)
        for row in results:
            print(row)
asyncio.run(main())
```


## Oracle

**Example:**

```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, Oracle, OraclePoolConfig, OracleSessionConfig, OracleSSLConfig

with Oracle(
    host="127.0.0.1",
    port=1521,
    user="system",
    password="oracle",
    servicename="xe",
    pool_config=OraclePoolConfig(
        pool_size=25,
        max_overflow=50,
        pool_recycle=3600,
        connection_timeout=30,
    ),
    session_config=OracleSessionConfig(
        echo=False,
        autoflush=False,
        expire_on_commit=False,
        autocommit=True,
    ),
    ssl_config=OracleSSLConfig(
        ssl_enabled=False,
        ssl_wallet_path=None,
    ),
) as session:
    stmt = sa.text("SELECT * FROM dual")
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

**Note:** Oracle only supports synchronous connections.



## MongoDB

**Example:**

```python
from ddcDatabases import MongoDB, MongoDBQueryConfig, MongoDBTLSConfig
from bson.objectid import ObjectId

with MongoDB(
    host="127.0.0.1",
    port=27017,
    user="admin",
    password="admin",
    database="admin",
    collection="test_collection",
    query_config=MongoDBQueryConfig(
        query={"_id": ObjectId("689c9f71dd642a68cfc60477")},
        sort_column="_id",
        sort_order="asc",          # asc or desc
        batch_size=2865,
        limit=0,
    ),
    tls_config=MongoDBTLSConfig(
        tls_enabled=False,
        tls_ca_cert_path=None,
        tls_cert_key_path=None,
        tls_allow_invalid_certificates=False,
    ),
) as cursor:
    for each in cursor:
        print(each)
```



# Database Engines

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
import asyncio
from ddcDatabases import PostgreSQL

async def main():
    async with PostgreSQL() as session:
        engine = session.bind
        # Use engine for advanced operations

asyncio.run(main())
```


# Database Utilities

The `DBUtils` and `DBUtilsAsync` classes provide convenient methods for common database operations with built-in retry support:

## Available Methods

```python
from ddcDatabases import DBUtils, DBUtilsAsync, PostgreSQL

# Synchronous utilities
with PostgreSQL() as session:
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)                # Returns list of RowMapping objects
    results = db_utils.fetchall(stmt, as_dict=True)  # Returns list of dictionaries
    value = db_utils.fetchvalue(stmt)                # Returns single value as string
    db_utils.insert(model_instance)                  # Insert into model table
    db_utils.deleteall(Model)                        # Delete all records from model
    db_utils.insertbulk(Model, data_list)            # Bulk insert from list of dictionaries
    db_utils.execute(stmt)                           # Execute any SQLAlchemy statement

# Asynchronous utilities (similar interface with await)
async with PostgreSQL() as session:
    db_utils_async = DBUtilsAsync(session)
    results = await db_utils_async.fetchall(stmt)
```

**Note:** Retry logic is configured at the database connection level using `operation_retry_config` (see [Retry Logic](#retry-logic) section).


# Logging

All database classes accept an optional `logger` parameter. By default, logs are silenced (NullHandler).

**Pass a custom logger to capture connection and retry messages:**

```python
import logging
from ddcDatabases import PostgreSQL, DBUtils

log = logging.getLogger("myapp")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

with PostgreSQL(host="localhost", database="mydb", logger=log) as session:
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
```

**Or configure the logging hierarchy (all modules propagate to the parent):**

```python
import logging
logging.getLogger("ddcDatabases").setLevel(logging.DEBUG)
logging.getLogger("ddcDatabases").addHandler(logging.StreamHandler())
```


# Development and Testing

Must have [UV](https://uv.run/docs/getting-started/installation) installed.

## Create DEV Environment and Running Tests

```shell
uv sync --all-extras --all-groups
poe tests
```

## Update DEV Environment Packages
This will update all packages dependencies

```shell
poe updatedev
```


## Building Wheel
This will update all packages, run linter, both unit and integration tests and finally build the wheel

```shell
poe build
```

## Optionals

```shell
# create a cprofile_unit.prof file from unit tests
poe profile
# create a cprofile_integration.prof file from integration tests
poe profile-integration
`````


# License

Released under the [MIT License](LICENSE)


# Support

If you find this project helpful, consider supporting development:

- [GitHub Sponsor](https://github.com/sponsors/ddc)
- [ko-fi](https://ko-fi.com/ddcsta)
- [PayPal](https://www.paypal.com/ncp/payment/6G9Z78QHUD4RJ)
