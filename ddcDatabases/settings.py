from functools import lru_cache
from typing import Callable, TypeVar
from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Type variable for generic settings factory
T = TypeVar('T', bound=BaseSettings)

# Constants
ECHO_DESCRIPTION = "Enable SQLAlchemy query logging"
AUTOFLUSH_DESCRIPTION = "Enable autoflush"
EXPIRE_ON_COMMIT_DESCRIPTION = "Enable expire on commit"
AUTOCOMMIT_DESCRIPTION = "Enable autocommit"
CONNECTION_TIMEOUT_DESCRIPTION = "Connection timeout in seconds"
POOL_RECYCLE_DESCRIPTION = "Pool recycle in seconds"
POOL_SIZE_DESCRIPTION = "Database connection pool size"
MAX_OVERFLOW_DESCRIPTION = "Maximum overflow connections for the pool"
HOST_DESCRIPTION = "Database host"
PORT_DESCRIPTION = "Database port"
USERNAME_DESCRIPTION = "Database username"
PASSWORD_DESCRIPTION = "Database password"
NAME_DESCRIPTION = "Database name"
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
    echo: bool = Field(default=False, description=ECHO_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="SQLITE_")


class PostgreSQLSettings(_BaseDBSettings):
    """PostgreSQL database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=HOST_DESCRIPTION)
    port: int = Field(default=5432, description=PORT_DESCRIPTION)
    user: str = Field(default="postgres", description=USERNAME_DESCRIPTION)
    password: str = Field(default="postgres", description=PASSWORD_DESCRIPTION)
    database: str = Field(default="postgres", description=NAME_DESCRIPTION)

    echo: bool = Field(default=False, description=ECHO_DESCRIPTION)
    autoflush: bool = Field(default=False, description=AUTOFLUSH_DESCRIPTION)
    expire_on_commit: bool = Field(default=False, description=EXPIRE_ON_COMMIT_DESCRIPTION)
    autocommit: bool = Field(default=False, description=AUTOCOMMIT_DESCRIPTION)
    connection_timeout: int = Field(default=30, description=CONNECTION_TIMEOUT_DESCRIPTION)
    pool_recycle: int = Field(default=3600, description=POOL_RECYCLE_DESCRIPTION)
    pool_size: int = Field(default=25, description=POOL_SIZE_DESCRIPTION)
    max_overflow: int = Field(default=50, description=MAX_OVERFLOW_DESCRIPTION)
    async_driver: str = Field(default="postgresql+asyncpg", description=ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="postgresql+psycopg2", description=SYNC_DATABASE_DRIVER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="POSTGRESQL_")


class MSSQLSettings(_BaseDBSettings):
    """Microsoft SQL Server settings with environment variable fallback."""

    host: str = Field(default="localhost", description=HOST_DESCRIPTION)
    port: int = Field(default=1433, description=PORT_DESCRIPTION)
    user: str = Field(default="sa", description=USERNAME_DESCRIPTION)
    password: str = Field(default="sa", description=PASSWORD_DESCRIPTION)
    db_schema: str = Field(default="dbo", description="Database schema")
    database: str = Field(default="master", description=NAME_DESCRIPTION)

    echo: bool = Field(default=False, description=ECHO_DESCRIPTION)
    autoflush: bool = Field(default=False, description=AUTOFLUSH_DESCRIPTION)
    expire_on_commit: bool = Field(default=False, description=EXPIRE_ON_COMMIT_DESCRIPTION)
    autocommit: bool = Field(default=False, description=AUTOCOMMIT_DESCRIPTION)
    connection_timeout: int = Field(default=30, description=CONNECTION_TIMEOUT_DESCRIPTION)
    pool_recycle: int = Field(default=3600, description=POOL_RECYCLE_DESCRIPTION)
    pool_size: int = Field(default=25, description="Connection pool size")
    max_overflow: int = Field(default=50, description="Max overflow connections")
    odbcdriver_version: int = Field(default=18, description="ODBC driver version")
    async_driver: str = Field(default="mssql+aioodbc", description=ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="mssql+pyodbc", description=SYNC_DATABASE_DRIVER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="MSSQL_")


class MySQLSettings(_BaseDBSettings):
    """MySQL database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=HOST_DESCRIPTION)
    port: int = Field(default=3306, description=PORT_DESCRIPTION)
    user: str = Field(default="root", description=USERNAME_DESCRIPTION)
    password: str = Field(default="root", description=PASSWORD_DESCRIPTION)
    database: str = Field(default="dev", description=NAME_DESCRIPTION)

    echo: bool = Field(default=False, description=ECHO_DESCRIPTION)
    autoflush: bool = Field(default=False, description=AUTOFLUSH_DESCRIPTION)
    expire_on_commit: bool = Field(default=False, description=EXPIRE_ON_COMMIT_DESCRIPTION)
    autocommit: bool = Field(default=True, description=AUTOCOMMIT_DESCRIPTION)
    connection_timeout: int = Field(default=30, description=CONNECTION_TIMEOUT_DESCRIPTION)
    pool_recycle: int = Field(default=3600, description=POOL_RECYCLE_DESCRIPTION)
    pool_size: int = Field(default=10, description=POOL_SIZE_DESCRIPTION)
    max_overflow: int = Field(default=20, description=MAX_OVERFLOW_DESCRIPTION)
    async_driver: str = Field(default="mysql+aiomysql", description=ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="mysql+pymysql", description=SYNC_DATABASE_DRIVER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="MYSQL_")


class MongoDBSettings(_BaseDBSettings):
    """MongoDB settings with environment variable fallback."""

    host: str = Field(default="localhost", description=HOST_DESCRIPTION)
    port: int = Field(default=27017, description=PORT_DESCRIPTION)
    user: str = Field(default="admin", description=USERNAME_DESCRIPTION)
    password: str = Field(default="admin", description=PASSWORD_DESCRIPTION)
    database: str = Field(default="admin", description=NAME_DESCRIPTION)

    batch_size: int = Field(default=2865, description="Batch size for operations")
    limit: int = Field(default=0, description="Query result limit (0 = no limit)")
    sync_driver: str = Field(default="mongodb", description="MongoDB driver")

    model_config = SettingsConfigDict(env_prefix="MONGODB_")


class OracleSettings(_BaseDBSettings):
    """Oracle database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=HOST_DESCRIPTION)
    port: int = Field(default=1521, description=PORT_DESCRIPTION)
    user: str = Field(default="system", description=USERNAME_DESCRIPTION)
    password: str = Field(default="oracle", description=PASSWORD_DESCRIPTION)
    servicename: str = Field(default="xe", description="Oracle service name")

    echo: bool = Field(default=False, description=ECHO_DESCRIPTION)
    autoflush: bool = Field(default=False, description=AUTOFLUSH_DESCRIPTION)
    expire_on_commit: bool = Field(default=False, description=EXPIRE_ON_COMMIT_DESCRIPTION)
    autocommit: bool = Field(default=True, description=AUTOCOMMIT_DESCRIPTION)
    connection_timeout: int = Field(default=30, description=CONNECTION_TIMEOUT_DESCRIPTION)
    pool_recycle: int = Field(default=3600, description=POOL_RECYCLE_DESCRIPTION)
    pool_size: int = Field(default=10, description=POOL_SIZE_DESCRIPTION)
    max_overflow: int = Field(default=20, description=MAX_OVERFLOW_DESCRIPTION)
    sync_driver: str = Field(default="oracle+cx_oracle", description="Oracle database driver")

    model_config = SettingsConfigDict(env_prefix="ORACLE_")


# Create optimized cached getter functions using the factory
get_sqlite_settings = _create_cached_settings_factory(SQLiteSettings)
get_postgresql_settings = _create_cached_settings_factory(PostgreSQLSettings)
get_mssql_settings = _create_cached_settings_factory(MSSQLSettings)
get_mysql_settings = _create_cached_settings_factory(MySQLSettings)
get_mongodb_settings = _create_cached_settings_factory(MongoDBSettings)
get_oracle_settings = _create_cached_settings_factory(OracleSettings)
