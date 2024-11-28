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
    username: Optional[str] = Field(default="postgres")
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
    username: Optional[str] = Field(default="sa")
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
