# Few Utility Functions

[![License](https://img.shields.io/github/license/ddc/ddcDatabases.svg?style=plastic)](https://github.com/ddc/ddcDatabases/blob/master/LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg?style=plastic)](https://www.python.org)
[![PyPi](https://img.shields.io/pypi/v/ddcDatabases.svg?style=plastic)](https://pypi.python.org/pypi/ddcDatabases)
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A//actions-badge.atrox.dev/ddc/ddcDatabases/badge?ref=main&style=plastic&label=build&logo=none)](https://actions-badge.atrox.dev/ddc/ddcDatabases/goto?ref=main)


# Install All databases dependencies
```shell
pip install ddcDatabases[all]
```

# Install MSSQL
```shell
pip install ddcDatabases[mssql]
```


# Install PostgreSQL
```shell
pip install ddcDatabases[pgsql]
```



# Databases
+ Parameters for all classes are declared as OPTIONAL falling back to [.env](.env.example) file
+ All examples are using [db_utils.py](ddcDatabases/db_utils.py)
+ By default, the MSSQL class will open a session to the database, but the engine can be available




## SQLITE
```
class Sqlite(
    file_path: Optional[str] = None,
    echo: Optional[bool] = None,
)
```

#### Session
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, Sqlite
with Sqlite() as session:
    utils = DBUtils(session)
    stmt = sa.select(Table).where(Table.id == 1)
    results = utils.fetchall(stmt)
    for row in results:
        print(row)
```

#### Sync Engine
```python
from ddcDatabases import Sqlite
with Sqlite().engine() as engine:
    ...
```





## MSSQL
```
class MSSQL(        
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    echo: Optional[bool] = None,
    pool_size: Optional[int] = None,
    max_overflow: Optional[int] = None
)
```

#### Sync Example
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, MSSQL
with MSSQL() as session:
    stmt = sa.select(Table).where(Table.id == 1)
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
    stmt = sa.select(Table).where(Table.id == 1)
    db_utils = DBUtilsAsync(session)
    results = await db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

#### Sync Engine
```python
from ddcDatabases import MSSQL
with MSSQL().engine() as engine:
    ...
```

#### Async Engine
```python
from ddcDatabases import MSSQL
async with MSSQL().async_engine() as engine:
    ...
```





## PostgreSQL
+ Using driver [psycopg2](https://pypi.org/project/psycopg2/) as default
```
class DBPostgres(
    host: Optional[str] = None,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    echo: Optional[bool] = None,
)
```

#### Sync Example
```python
import sqlalchemy as sa
from ddcDatabases import DBUtils, PostgreSQL
with PostgreSQL() as session:
    stmt = sa.select(Table).where(Table.id == 1)
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
    stmt = sa.select(Table).where(Table.id == 1)
    db_utils = DBUtilsAsync(session)
    results = await db_utils.fetchall(stmt)
    for row in results:
        print(row)
```

#### Sync Engine
```python
from ddcDatabases import PostgreSQL
with PostgreSQL().engine() as engine:
    ...
```

#### Async Engine
```python
from ddcDatabases import PostgreSQL
async with PostgreSQL().async_engine() as engine:
    ...
```




## DBUtils and DBUtilsAsync
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


### Run Tests and Get Coverage Report
```shell
poetry run coverage run --omit=./tests/* --source=./ddcDatabases -m pytest -v && poetry run coverage report
```



# License
Released under the [MIT License](LICENSE)
