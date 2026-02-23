import pytest
import time
from sqlalchemy import Boolean, Identity, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Testcontainer image versions
POSTGRES_IMAGE = "postgres:latest"
MYSQL_IMAGE = "mysql:latest"
MONGODB_IMAGE = "mongo:8.0"
MARIADB_IMAGE = "mariadb:latest"
ORACLE_IMAGE = "gvenzl/oracle-free:slim-faststart"
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"


class Base(DeclarativeBase):
    pass


class IntegrationModel(Base):
    __tablename__ = "integration_test"
    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


@pytest.fixture(scope="session")
def postgres_container():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE) as pg:
        yield pg


@pytest.fixture(scope="session")
def mysql_container():
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer(MYSQL_IMAGE) as mysql:
        yield mysql


@pytest.fixture(scope="session")
def mssql_container():
    from testcontainers.mssql import SqlServerContainer

    with SqlServerContainer(MSSQL_IMAGE, password="Strong@Pass123") as mssql:
        yield mssql


@pytest.fixture(scope="session")
def mongodb_container():
    from testcontainers.mongodb import MongoDbContainer

    max_attempts = 3
    last_exc = None
    for attempt in range(max_attempts):
        try:
            container = MongoDbContainer(MONGODB_IMAGE)
            container.start()
            break
        except Exception as exc:
            last_exc = exc
            if attempt < max_attempts - 1:
                time.sleep(2)
    else:
        raise last_exc
    yield container
    container.stop()


@pytest.fixture(scope="session")
def mariadb_container():
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer(MARIADB_IMAGE) as mariadb:
        yield mariadb


@pytest.fixture(scope="session")
def oracle_container():
    from testcontainers.oracle import OracleDbContainer

    with OracleDbContainer(ORACLE_IMAGE) as oracle:
        yield oracle
