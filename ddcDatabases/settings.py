from functools import lru_cache
from typing import Callable, TypeVar
from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Type variable for generic settings factory
T = TypeVar('T', bound=BaseSettings)

# Constants
SQLALCHEMY_ECHO_DESCRIPTION = "Enable SQLAlchemy query logging"
DATABASE_HOST_DESCRIPTION = "Database host"
DATABASE_PORT_DESCRIPTION = "Database port"
DATABASE_USERNAME_DESCRIPTION = "Database username"
DATABASE_PASSWORD_DESCRIPTION = "Database password"
DATABASE_NAME_DESCRIPTION = "Database name"
ASYNC_DATABASE_DRIVER_DESCRIPTION = "Async database driver"
SYNC_DATABASE_DRIVER_DESCRIPTION = "Sync database driver"

# Lazy loading flag for dotenv - thread-safe singleton pattern
_dotenv_loaded = False


def _ensure_dotenv_loaded() -> None:
    """Ensure dotenv is loaded only once in a thread-safe manner."""
    global _dotenv_loaded
    if not _dotenv_loaded:
        load_dotenv()
        _dotenv_loaded = True


def _create_cached_settings_factory(settings_class: type[T]) -> Callable[[], T]:
    """Factory function to create cached settings getters with proper type hints."""

    @lru_cache(maxsize=1)
    def get_settings() -> T:
        _ensure_dotenv_loaded()
        return settings_class()

    return get_settings


class _BaseDBSettings(BaseSettings):
    """Base class for database settings with common configuration."""

    model_config = SettingsConfigDict(env_file=".env", extra="allow")


class SQLiteSettings(_BaseDBSettings):
    """SQLite database settings with environment variable fallback."""

    file_path: str = Field(default="sqlite.db", description="Path to SQLite database file")
    echo: bool = Field(default=False, description=SQLALCHEMY_ECHO_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="SQLITE_")


class PostgreSQLSettings(_BaseDBSettings):
    """PostgreSQL database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=DATABASE_HOST_DESCRIPTION)
    port: int = Field(default=5432, description=DATABASE_PORT_DESCRIPTION)
    user: str = Field(default="postgres", description=DATABASE_USERNAME_DESCRIPTION)
    password: str = Field(default="postgres", description=DATABASE_PASSWORD_DESCRIPTION)
    database: str = Field(default="postgres", description=DATABASE_NAME_DESCRIPTION)

    echo: bool = Field(default=False, description=SQLALCHEMY_ECHO_DESCRIPTION)
    async_driver: str = Field(default="postgresql+asyncpg", description=ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="postgresql+psycopg2", description=SYNC_DATABASE_DRIVER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="POSTGRESQL_")


class MSSQLSettings(_BaseDBSettings):
    """Microsoft SQL Server settings with environment variable fallback."""

    host: str = Field(default="localhost", description=DATABASE_HOST_DESCRIPTION)
    port: int = Field(default=1433, description=DATABASE_PORT_DESCRIPTION)
    user: str = Field(default="sa", description=DATABASE_USERNAME_DESCRIPTION)
    password: str = Field(default="sa", description=DATABASE_PASSWORD_DESCRIPTION)
    db_schema: str = Field(default="dbo", description="Database schema")
    database: str = Field(default="master", description=DATABASE_NAME_DESCRIPTION)

    echo: bool = Field(default=False, description=SQLALCHEMY_ECHO_DESCRIPTION)
    pool_size: int = Field(default=20, description="Connection pool size")
    max_overflow: int = Field(default=10, description="Max overflow connections")
    odbcdriver_version: int = Field(default=18, description="ODBC driver version")
    async_driver: str = Field(default="mssql+aioodbc", description=ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="mssql+pyodbc", description=SYNC_DATABASE_DRIVER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="MSSQL_")


class MySQLSettings(_BaseDBSettings):
    """MySQL database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=DATABASE_HOST_DESCRIPTION)
    port: int = Field(default=3306, description=DATABASE_PORT_DESCRIPTION)
    user: str = Field(default="root", description=DATABASE_USERNAME_DESCRIPTION)
    password: str = Field(default="root", description=DATABASE_PASSWORD_DESCRIPTION)
    database: str = Field(default="dev", description=DATABASE_NAME_DESCRIPTION)

    echo: bool = Field(default=False, description=SQLALCHEMY_ECHO_DESCRIPTION)
    async_driver: str = Field(default="mysql+aiomysql", description=ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="mysql+pymysql", description=SYNC_DATABASE_DRIVER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="MYSQL_")


class MongoDBSettings(_BaseDBSettings):
    """MongoDB settings with environment variable fallback."""

    host: str = Field(default="localhost", description=DATABASE_HOST_DESCRIPTION)
    port: int = Field(default=27017, description=DATABASE_PORT_DESCRIPTION)
    user: str = Field(default="admin", description=DATABASE_USERNAME_DESCRIPTION)
    password: str = Field(default="admin", description=DATABASE_PASSWORD_DESCRIPTION)
    database: str = Field(default="admin", description=DATABASE_NAME_DESCRIPTION)

    batch_size: int = Field(default=2865, description="Batch size for operations")
    limit: int = Field(default=0, description="Query result limit (0 = no limit)")
    sync_driver: str = Field(default="mongodb", description="MongoDB driver")

    model_config = SettingsConfigDict(env_prefix="MONGODB_")


class OracleSettings(_BaseDBSettings):
    """Oracle database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=DATABASE_HOST_DESCRIPTION)
    port: int = Field(default=1521, description=DATABASE_PORT_DESCRIPTION)
    user: str = Field(default="system", description=DATABASE_USERNAME_DESCRIPTION)
    password: str = Field(default="oracle", description=DATABASE_PASSWORD_DESCRIPTION)
    servicename: str = Field(default="xe", description="Oracle service name")

    echo: bool = Field(default=False, description=SQLALCHEMY_ECHO_DESCRIPTION)
    sync_driver: str = Field(default="oracle+cx_oracle", description="Oracle database driver")

    model_config = SettingsConfigDict(env_prefix="ORACLE_")


# Create optimized cached getter functions using the factory
get_sqlite_settings = _create_cached_settings_factory(SQLiteSettings)
get_postgresql_settings = _create_cached_settings_factory(PostgreSQLSettings)
get_mssql_settings = _create_cached_settings_factory(MSSQLSettings)
get_mysql_settings = _create_cached_settings_factory(MySQLSettings)
get_mongodb_settings = _create_cached_settings_factory(MongoDBSettings)
get_oracle_settings = _create_cached_settings_factory(OracleSettings)
