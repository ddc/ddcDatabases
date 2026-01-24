import pytest
from sqlalchemy import Boolean, Identity, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class IntegrationModel(Base):
    __tablename__ = "integration_test"
    id: Mapped[int] = mapped_column(Identity(always=True), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)


@pytest.fixture(scope="module")
def postgres_container():
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:18-alpine") as pg:
        yield pg


@pytest.fixture(scope="module")
def mysql_container():
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer("mysql:9") as mysql:
        yield mysql


@pytest.fixture(scope="module")
def mssql_container():
    from testcontainers.mssql import SqlServerContainer

    with SqlServerContainer("mcr.microsoft.com/mssql/server:2022-latest", password="Strong@Pass123") as mssql:
        yield mssql


@pytest.fixture(scope="module")
def mongodb_container():
    from testcontainers.mongodb import MongoDbContainer

    with MongoDbContainer("mongo:8") as mongo:
        yield mongo


@pytest.fixture(scope="module")
def mariadb_container():
    from testcontainers.mysql import MySqlContainer

    with MySqlContainer("mariadb:11") as mariadb:
        yield mariadb


@pytest.fixture(scope="module")
def oracle_container():
    from testcontainers.oracle import OracleDbContainer

    with OracleDbContainer("gvenzl/oracle-free:slim") as oracle:
        yield oracle
