# Few Utility Functions

[![License](https://img.shields.io/github/license/ddc/ddcDatabases.svg?style=plastic)](https://github.com/ddc/ddcDatabases/blob/master/LICENSE)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg?style=plastic)](https://www.python.org)
[![PyPi](https://img.shields.io/pypi/v/ddcDatabases.svg?style=plastic)](https://pypi.python.org/pypi/ddcDatabases)
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A//actions-badge.atrox.dev/ddc/ddcDatabases/badge?ref=main&style=plastic&label=build&logo=none)](https://actions-badge.atrox.dev/ddc/ddcDatabases/goto?ref=main)


# Install
```shell
pip install ddcDatabases
```


# Databases
## DBSQLITE
```
class DBSqlite(db_file_path: str, batch_size=100, echo=False, future=True)
```

```python
import sqlalchemy as sa
from ddcDatabases import DBSqlite, DBUtils
dbsqlite = DBSqlite(database_file_path)
with dbsqlite.session() as session:
    stmt = sa.select(Table).where(Table.id == 1)
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
```


## DBPOSTGRES
  + Using driver "psycopg2" as default
```
class DBPostgres(future=True, echo=False, drivername="psycopg2", **kwargs)
```

```python
import sqlalchemy as sa
from ddcDatabases import DBPostgres, DBUtils
db_configs = {
    "username": username,
    "password": password,
    "host": host,
    "port": port,
    "database": database
}
dbpostgres = DBPostgres(**db_configs)
with dbpostgres.session() as session:
    stmt = sa.select(Table).where(Table.id == 1)
    db_utils = DBUtils(session)
    results = db_utils.fetchall(stmt)
```

+ DBUTILS
  + Uses SQLAlchemy statements
```python
from ddcDatabases import DBUtils
db_utils = DBUtils(session)
db_utils.add(stmt)
db_utils.execute(stmt)
db_utils.fetchall(stmt)
db_utils.fetchone(stmt)
db_utils.fetch_value(stmt)
```


## DBPOSTGRES ASYNC
  + Using driver "asyncpg"
```
class DBPostgresAsync(future=True, echo=False, drivername="asyncpg", **kwargs)
```

```python
import sqlalchemy as sa
from ddcDatabases import DBPostgresAsync, DBUtilsAsync
db_configs = {
    "username": username,
    "password": password,
    "host": host,
    "port": port,
    "database": database
}
dbpostgres = DBPostgresAsync(**db_configs)
async with dbpostgres.session() as session:
    stmt = sa.select(Table).where(Table.id == 1)
    db_utils = DBUtilsAsync(session)
    results = await db_utils.fetchall(stmt)
```

+ DBUTILS ASYNC
  + Uses SQLAlchemy statements
```python
from ddcDatabases import DBUtilsAsync
db_utils = DBUtilsAsync(session)
await db_utils.add(stmt)
await db_utils.execute(stmt)
await db_utils.fetchall(stmt)
await db_utils.fetchone(stmt)
await db_utils.fetch_value(stmt)
```


# Source Code
### Build
```shell
poetry build
```


### Run Tests
```shell
poetry run coverage run -m pytest -v
```


### Get Coverage Report
```shell
poetry run coverage report
```


# License
Released under the [MIT License](LICENSE)
