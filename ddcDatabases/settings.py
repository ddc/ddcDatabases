# -*- coding: utf-8 -*-
from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


load_dotenv()


class MSSQLSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    host: str = Field(default="localhost")
    port: int = Field(default=1433)
    user: str = Field(default="sa")
    password: str = Field(default=None)
    db_schema: str = Field(default="dbo")
    database: str = Field(default="master")

    echo: bool = Field(default=False)
    pool_size: int = Field(default=20)
    overflow: int = Field(default=10)
    odbcdriver_version: int = Field(default=18)
    async_driver: str = Field(default="mssql+aioodbc")
    sync_driver: str = Field(default="mssql+pyodbc")

    model_config = SettingsConfigDict(env_prefix="MSSQL_", env_file=".env", extra="allow")


class PostgreSQLSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    host: str = Field(default="localhost")
    port: int = Field(default=1433)
    user: str = Field(default="sa")
    password: str = Field(default=None)
    db_schema: str = Field(default="dbo")
    database: str = Field(default="master")

    model_config = SettingsConfigDict(env_prefix="POSTGRESQL_", env_file=".env", extra="allow")


class SQLiteSettings(BaseSettings):
    """settings defined here with fallback to reading ENV variables"""

    file_path: str = Field(default="sqlite.db")
    batch_size: int = Field(default=50)
    echo: bool = Field(default=False)
    future: bool = Field(default=True)
    isolation_level: str = Field(default="AUTOCOMMIT")

    model_config = SettingsConfigDict(env_prefix="SQLITE_", env_file=".env", extra="allow")
