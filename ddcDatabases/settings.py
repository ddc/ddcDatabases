# -*- coding: utf-8 -*-
from typing import Optional
from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


load_dotenv()


class SQLiteSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    file_path: Optional[str] = Field(default="sqlite.db")
    echo: Optional[bool] = Field(default=False)

    model_config = SettingsConfigDict(env_prefix="SQLITE_", env_file=".env", extra="allow")


class PostgreSQLSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    host: Optional[str] = Field(default="localhost")
    port: Optional[int] = Field(default=5432)
    user: Optional[str] = Field(default="postgres")
    password: Optional[str] = Field(default="postgres")
    database: Optional[str] = Field(default="postgres")

    echo: Optional[bool] = Field(default=False)
    async_driver: Optional[str] = Field(default="postgresql+asyncpg")
    sync_driver: Optional[str] = Field(default="postgresql+psycopg2")

    model_config = SettingsConfigDict(env_prefix="POSTGRESQL_", env_file=".env", extra="allow")


class MSSQLSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    host: Optional[str] = Field(default="localhost")
    port: Optional[int] = Field(default=1433)
    user: Optional[str] = Field(default="sa")
    password: Optional[str] = Field(default=None)
    db_schema: Optional[str] = Field(default="dbo")
    database: Optional[str] = Field(default="master")

    echo: Optional[bool] = Field(default=False)
    pool_size: Optional[int] = Field(default=20)
    max_overflow: Optional[int] = Field(default=10)
    odbcdriver_version: Optional[int] = Field(default=18)
    async_driver: Optional[str] = Field(default="mssql+aioodbc")
    sync_driver: Optional[str] = Field(default="mssql+pyodbc")

    model_config = SettingsConfigDict(env_prefix="MSSQL_", env_file=".env", extra="allow")


class MySQLSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    host: Optional[str] = Field(default="localhost")
    port: Optional[int] = Field(default=3306)
    user: Optional[str] = Field(default="root")
    password: Optional[str] = Field(default="root")
    database: Optional[str] = Field(default="dev")

    echo: Optional[bool] = Field(default=False)
    async_driver: Optional[str] = Field(default="mysql+aiomysql")
    sync_driver: Optional[str] = Field(default="mysql+pymysql")

    model_config = SettingsConfigDict(env_prefix="MYSQL_", env_file=".env", extra="allow")


class MongoDBSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    host: Optional[str] = Field(default="localhost")
    port: Optional[int] = Field(default=27017)
    user: Optional[str] = Field(default="admin")
    password: Optional[str] = Field(default="admin")
    database: Optional[str] = Field(default="admin")

    batch_size: Optional[int] = Field(default=2865)
    limit: Optional[int] = Field(default=0)
    sync_driver: Optional[str] = Field(default="mongodb")

    model_config = SettingsConfigDict(env_prefix="MONGODB_", env_file=".env", extra="allow")


class OracleSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    host: Optional[str] = Field(default="localhost")
    port: Optional[int] = Field(default=1521)
    user: Optional[str] = Field(default="system")
    password: Optional[str] = Field(default="oracle")
    servicename: Optional[str] = Field(default="xe")

    echo: Optional[bool] = Field(default=False)
    sync_driver: Optional[str] = Field(default="oracle+cx_oracle")

    model_config = SettingsConfigDict(env_prefix="ORACLE_", env_file=".env", extra="allow")
