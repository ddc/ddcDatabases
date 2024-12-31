# Databases Session Connections and ORM Queries

[![Donate](https://img.shields.io/badge/Donate-PayPal-brightgreen.svg?style=plastic)](https://www.paypal.com/ncp/payment/6G9Z78QHUD4RJ)
[![License](https://img.shields.io/pypi/l/ddcDatabases)](https://github.com/ddc/ddcDatabases/blob/master/LICENSE)
[![PyPi](https://img.shields.io/pypi/v/ddcDatabases.svg)](https://pypi.python.org/pypi/ddcDatabases)
[![PyPI Downloads](https://static.pepy.tech/badge/ddcDatabases)](https://pepy.tech/projects/ddcDatabases)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A//actions-badge.atrox.dev/ddc/ddcDatabases/badge?ref=main&label=build&logo=none)](https://actions-badge.atrox.dev/ddc/ddcDatabases/goto?ref=main)
[![Python](https://img.shields.io/pypi/pyversions/ddcDatabases.svg)](https://www.python.org)

[//]: # ([![codecov]&#40;https://codecov.io/github/ddc/ddcDatabases/graph/badge.svg?token=E942EZII4Q&#41;]&#40;https://codecov.io/github/ddc/ddcDatabases&#41;)



# Install only sqlite
```shell
pip install ddcDatabases
```



# Install All databases
```shell
pip install ddcDatabases[all]
```



# Install MSSQL
```shell
pip install ddcDatabases[mssql]
```



# Install MYSQL
```shell
pip install ddcDatabases[mysql]
```



# Install PostgreSQL
```shell
pip install ddcDatabases[pgsql]
```



# Install Oracle
```shell
pip install ddcDatabases[oracle]
```



# Install MONGODB
```shell
pip install ddcDatabases[mongodb]
```



# Databases
+ Parameters for all classes are declared as OPTIONAL falling back to [.env](./ddcDatabases/.env.example)  file variables
+ All examples are using [db_utils.py](ddcDatabases/db_utils.py)
+ By default, the MSSQL class will open a session to the database, but the engine can be available at `session.bind`
+ SYNC sessions defaults:
  + `autoflush is True`
  + `expire_on_commit is True`
  + `echo is False`
+ ASYNC sessions defaults:
  + `autoflush is True`
  + `expire_on_commit is False`
  + `echo is False`



# SQLITE
```
class Sqlite(
    filepath: Optional[str] = None,
    echo: Optional[bool] = None,
    autoflush: Optional[bool] = None,
    expire_on_commit: Optional[bool] = None,
    extra_engine_args: Optional[dict] = None,
)
```

#### Session
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, Sqlite
with Sqlite() as session:
    utils = DBUtils(session)
    stmt = sa.select(TableModel).where(TableModel.id == 1)
    results = utils.fetchall(stmt)
    for row in results:
        print(row)
```





# MSSQL
```
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

#### Sync Example
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MSSQL
with MSSQL() as session:
    stmt = sa.select(TableModel).where(TableModel.id == 1)
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

#### Async Example
```python
import sqlalchemy as sa
from ddcDatabases import DBUtilsAsync, MSSQL
async with MSSQL() as session:
    stmt = sa.select(TableModel).where(TableModel.id == 1)
    db_utils = DBUtilsAsync(session)
    results = await db_utils.fetchall(stmt)
    for row in results:
        print(row)
```






# PostgreSQL or MySQL
```
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

#### Sync Examples
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, PostgreSQL
with PostgreSQL() as session:
    stmt = sa.select(TableModel).where(TableModel.id == 1)
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MySQL
with MySQL() as session:
    stmt = sa.text("SELECT * FROM users")
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```


#### Async Example
```python
import sqlalchemy as sa
from ddcDatabases import DBUtilsAsync, PostgreSQL
async with PostgreSQL() as session:
    stmt = sa.select(TableModel).where(TableModel.id == 1)
    db_utils = DBUtilsAsync(session)
    results = await db_utils.fetchall(stmt)
    for row in results:
        print(row)
```




# Oracle
```
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
```
#### Sync Example using arguments instead of .env file
```python
credentials = {
    "host": "127.0.0.1",
    "user": "system",
    "password": "oracle",
    "servicename": "xe",
    "echo": False,
}

import sqlalchemy as sa
from ddcDatabases import DBUtils, Oracle
with Oracle(**credentials) as session:
    stmt = sa.text("SELECT * FROM system.help")
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
    for row in results:
        print(row)
```








# MongoDB
```
class PostgreSQL(
    host: Optional[str] = None,
    port: Optional[int] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    batch_size: Optional[int] = None,
    limit: Optional[int] = None,
)
```
#### Sync Example using arguments instead of .env file
```python
credentials = {
    "host": "127.0.0.1",
    "user": "admin",
    "password": "admin",
    "database": "admin",
}

from ddcDatabases.mongodb import MongoDB
from bson.objectid import ObjectId
with MongoDB(**credentials) as mongodb:
    query = {"_id": ObjectId("6772cf60f27e7e068e9d8985")}
    collection = "movies"
    with mongodb.cursor(collection, query) as cursor:
        for each in cursor:
            print(each)
```








# ORM Engines
Using PostgreSQL as example

#### Sync Engine
```python
from ddcDatabases import PostgreSQL
with PostgreSQL() as session:
    engine = session.bind
    ...
```

#### Async Engine
```python
from ddcDatabases import PostgreSQL
async with PostgreSQL() as session:
    engine = await session.bind
    ...
```




# ORM DBUtils and DBUtilsAsync
+ Take an open session as parameter
+ Can use SQLAlchemy statements
+ Execute function can be used to update, insert or any SQLAlchemy.text
```python
from ddcDatabases import DBUtils
db_utils = DBUtils(session)
db_utils.fetchall(stmt)                     # returns a list of RowMapping
db_utils.fetchvalue(stmt)                   # fetch a single value, returning as string
db_utils.insert(stmt)                       # insert into model table
db_utils.deleteall(model)                   # delete all records from model
db_utils.insertbulk(model, list[dict])      # insert records into model from a list of dicts
db_utils.execute(stmt)                      # this is the actual execute from session
```




# Source Code
### Build
```shell
poetry build -f wheel
```



# Run Tests and Get Coverage Report using Poe
```shell
poetry update --with test
poe tests
```



# License
Released under the [MIT License](LICENSE)



# Buy me a cup of coffee
+ [GitHub Sponsor](https://github.com/sponsors/ddc)
+ [ko-fi](https://ko-fi.com/ddcsta)
+ [Paypal](https://www.paypal.com/ncp/payment/6G9Z78QHUD4RJ)
