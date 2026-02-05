import pytest
from sqlalchemy import Boolean, Identity, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Testcontainer image versions
POSTGRES_IMAGE = "postgres:18-alpine"
MYSQL_IMAGE = "mysql:9"
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
MONGODB_IMAGE = "mongo:8"
MARIADB_IMAGE = "mariadb:12"
ORACLE_IMAGE = "gvenzl/oracle-free:23-slim"


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

    with MongoDbContainer(MONGODB_IMAGE) as mongo:
        yield mongo


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
