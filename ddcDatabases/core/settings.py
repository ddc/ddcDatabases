from .constants import SettingsMessages as Msg
from dotenv import load_dotenv
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Callable, TypeVar

# Type variable for generic settings factory
T = TypeVar('T', bound=BaseSettings)

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

    file_path: str = Field(default="sqlite.db", description=Msg.SQLITE_FILE_PATH_DESCRIPTION)
    echo: bool = Field(default=False, description=Msg.ECHO_DESCRIPTION)

    # Connection Retry settings (minimal for file-based database)
    conn_enable_retry: bool = Field(default=False, description=Msg.ENABLE_RETRY_DESCRIPTION)
    conn_max_retries: int = Field(default=1, description=Msg.MAX_RETRIES_DESCRIPTION)
    conn_initial_retry_delay: float = Field(default=1.0, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    conn_max_retry_delay: float = Field(default=30.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)

    # Operation Retry settings
    op_enable_retry: bool = Field(default=False, description=Msg.ENABLE_RETRY_DESCRIPTION)
    op_max_retries: int = Field(default=1, description=Msg.MAX_RETRIES_DESCRIPTION)
    op_initial_retry_delay: float = Field(default=0.5, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    op_max_retry_delay: float = Field(default=10.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    op_jitter: float = Field(default=0.1, description=Msg.JITTER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="SQLITE_")


class PostgreSQLSettings(_BaseDBSettings):
    """PostgreSQL database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=Msg.HOST_DESCRIPTION)
    port: int = Field(default=5432, description=Msg.PORT_DESCRIPTION)
    user: str = Field(default="postgres", description=Msg.USERNAME_DESCRIPTION)
    password: str = Field(default="postgres", description=Msg.PASSWORD_DESCRIPTION)
    database: str = Field(default="postgres", description=Msg.NAME_DESCRIPTION)
    schema: str | None = Field(default="public", description=Msg.SCHEMA_DESCRIPTION)

    echo: bool = Field(default=False, description=Msg.ECHO_DESCRIPTION)
    autoflush: bool = Field(default=False, description=Msg.AUTOFLUSH_DESCRIPTION)
    expire_on_commit: bool = Field(default=False, description=Msg.EXPIRE_ON_COMMIT_DESCRIPTION)
    autocommit: bool = Field(default=False, description=Msg.AUTOCOMMIT_DESCRIPTION)
    connection_timeout: int = Field(default=30, description=Msg.CONNECTION_TIMEOUT_DESCRIPTION)
    pool_recycle: int = Field(default=3600, description=Msg.POOL_RECYCLE_DESCRIPTION)
    pool_size: int = Field(default=25, description=Msg.POOL_SIZE_DESCRIPTION)
    max_overflow: int = Field(default=50, description=Msg.MAX_OVERFLOW_DESCRIPTION)
    async_driver: str = Field(default="postgresql+asyncpg", description=Msg.ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="postgresql+psycopg", description=Msg.SYNC_DATABASE_DRIVER_DESCRIPTION)

    # SSL settings
    ssl_mode: str = Field(default="disable", description=Msg.SSL_MODE_DESCRIPTION)
    ssl_ca_cert_path: str | None = Field(default=None, description=Msg.SSL_CA_CERT_PATH_DESCRIPTION)
    ssl_client_cert_path: str | None = Field(default=None, description=Msg.SSL_CLIENT_CERT_PATH_DESCRIPTION)
    ssl_client_key_path: str | None = Field(default=None, description=Msg.SSL_CLIENT_KEY_PATH_DESCRIPTION)

    # Connection Retry settings
    conn_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    conn_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    conn_initial_retry_delay: float = Field(default=1.0, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    conn_max_retry_delay: float = Field(default=30.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    conn_disconnect_idle_timeout: int = Field(default=300, description=Msg.DISCONNECT_IDLE_TIMEOUT_DESCRIPTION)

    # Operation Retry settings
    op_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    op_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    op_initial_retry_delay: float = Field(default=0.5, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    op_max_retry_delay: float = Field(default=10.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    op_jitter: float = Field(default=0.1, description=Msg.JITTER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="POSTGRESQL_")


class MSSQLSettings(_BaseDBSettings):
    """Microsoft SQL Server settings with environment variable fallback."""

    host: str = Field(default="localhost", description=Msg.HOST_DESCRIPTION)
    port: int = Field(default=1433, description=Msg.PORT_DESCRIPTION)
    user: str = Field(default="sa", description=Msg.USERNAME_DESCRIPTION)
    password: str = Field(default="sa", description=Msg.PASSWORD_DESCRIPTION)
    database: str = Field(default="master", description=Msg.NAME_DESCRIPTION)
    schema: str = Field(default="dbo", description=Msg.SCHEMA_DESCRIPTION)

    echo: bool = Field(default=False, description=Msg.ECHO_DESCRIPTION)
    autoflush: bool = Field(default=False, description=Msg.AUTOFLUSH_DESCRIPTION)
    expire_on_commit: bool = Field(default=False, description=Msg.EXPIRE_ON_COMMIT_DESCRIPTION)
    autocommit: bool = Field(default=False, description=Msg.AUTOCOMMIT_DESCRIPTION)
    connection_timeout: int = Field(default=30, description=Msg.CONNECTION_TIMEOUT_DESCRIPTION)
    pool_recycle: int = Field(default=3600, description=Msg.POOL_RECYCLE_DESCRIPTION)
    pool_size: int = Field(default=25, description=Msg.POOL_SIZE_DESCRIPTION)
    max_overflow: int = Field(default=50, description=Msg.MAX_OVERFLOW_DESCRIPTION)
    odbcdriver_version: int = Field(default=18, description=Msg.ODBC_DRIVER_VERSION_DESCRIPTION)
    async_driver: str = Field(default="mssql+aioodbc", description=Msg.ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="mssql+pyodbc", description=Msg.SYNC_DATABASE_DRIVER_DESCRIPTION)

    # SSL settings
    ssl_encrypt: bool = Field(default=False, description=Msg.SSL_ENCRYPT_DESCRIPTION)
    ssl_trust_server_certificate: bool = Field(default=True, description=Msg.SSL_TRUST_SERVER_CERTIFICATE_DESCRIPTION)
    ssl_ca_cert_path: str | None = Field(default=None, description=Msg.SSL_CA_CERT_PATH_DESCRIPTION)

    # Connection Retry settings
    conn_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    conn_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    conn_initial_retry_delay: float = Field(default=1.0, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    conn_max_retry_delay: float = Field(default=30.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    conn_disconnect_idle_timeout: int = Field(default=300, description=Msg.DISCONNECT_IDLE_TIMEOUT_DESCRIPTION)

    # Operation Retry settings
    op_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    op_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    op_initial_retry_delay: float = Field(default=0.5, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    op_max_retry_delay: float = Field(default=10.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    op_jitter: float = Field(default=0.1, description=Msg.JITTER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="MSSQL_")


class MySQLSettings(_BaseDBSettings):
    """MySQL database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=Msg.HOST_DESCRIPTION)
    port: int = Field(default=3306, description=Msg.PORT_DESCRIPTION)
    user: str = Field(default="root", description=Msg.USERNAME_DESCRIPTION)
    password: str = Field(default="root", description=Msg.PASSWORD_DESCRIPTION)
    database: str = Field(default="dev", description=Msg.NAME_DESCRIPTION)

    echo: bool = Field(default=False, description=Msg.ECHO_DESCRIPTION)
    autoflush: bool = Field(default=False, description=Msg.AUTOFLUSH_DESCRIPTION)
    expire_on_commit: bool = Field(default=False, description=Msg.EXPIRE_ON_COMMIT_DESCRIPTION)
    autocommit: bool = Field(default=True, description=Msg.AUTOCOMMIT_DESCRIPTION)
    connection_timeout: int = Field(default=30, description=Msg.CONNECTION_TIMEOUT_DESCRIPTION)
    pool_recycle: int = Field(default=3600, description=Msg.POOL_RECYCLE_DESCRIPTION)
    pool_size: int = Field(default=10, description=Msg.POOL_SIZE_DESCRIPTION)
    max_overflow: int = Field(default=20, description=Msg.MAX_OVERFLOW_DESCRIPTION)
    async_driver: str = Field(default="mysql+aiomysql", description=Msg.ASYNC_DATABASE_DRIVER_DESCRIPTION)
    sync_driver: str = Field(default="mysql+mysqldb", description=Msg.SYNC_DATABASE_DRIVER_DESCRIPTION)

    # SSL settings
    ssl_mode: str = Field(default="DISABLED", description=Msg.SSL_MODE_DESCRIPTION)
    ssl_ca_cert_path: str | None = Field(default=None, description=Msg.SSL_CA_CERT_PATH_DESCRIPTION)
    ssl_client_cert_path: str | None = Field(default=None, description=Msg.SSL_CLIENT_CERT_PATH_DESCRIPTION)
    ssl_client_key_path: str | None = Field(default=None, description=Msg.SSL_CLIENT_KEY_PATH_DESCRIPTION)

    # Connection Retry settings
    conn_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    conn_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    conn_initial_retry_delay: float = Field(default=1.0, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    conn_max_retry_delay: float = Field(default=30.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    conn_disconnect_idle_timeout: int = Field(default=300, description=Msg.DISCONNECT_IDLE_TIMEOUT_DESCRIPTION)

    # Operation Retry settings
    op_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    op_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    op_initial_retry_delay: float = Field(default=0.5, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    op_max_retry_delay: float = Field(default=10.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    op_jitter: float = Field(default=0.1, description=Msg.JITTER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="MYSQL_")


class MongoDBSettings(_BaseDBSettings):
    """MongoDB settings with environment variable fallback."""

    host: str = Field(default="localhost", description=Msg.HOST_DESCRIPTION)
    port: int = Field(default=27017, description=Msg.PORT_DESCRIPTION)
    user: str = Field(default="admin", description=Msg.USERNAME_DESCRIPTION)
    password: str = Field(default="admin", description=Msg.PASSWORD_DESCRIPTION)
    database: str = Field(default="admin", description=Msg.NAME_DESCRIPTION)

    batch_size: int = Field(default=2865, description=Msg.BATCH_SIZE_DESCRIPTION)
    limit: int = Field(default=0, description=Msg.QUERY_LIMIT_DESCRIPTION)
    driver: str = Field(default="mongodb", description=Msg.MONGODB_DRIVER_DESCRIPTION)

    # TLS settings
    tls_enabled: bool = Field(default=False, description=Msg.TLS_ENABLED_DESCRIPTION)
    tls_ca_cert_path: str | None = Field(default=None, description=Msg.TLS_CA_CERT_PATH_DESCRIPTION)
    tls_cert_key_path: str | None = Field(default=None, description=Msg.TLS_CERT_KEY_PATH_DESCRIPTION)
    tls_allow_invalid_certificates: bool = Field(
        default=False, description=Msg.TLS_ALLOW_INVALID_CERTIFICATES_DESCRIPTION
    )

    # Connection Retry settings
    conn_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    conn_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    conn_initial_retry_delay: float = Field(default=1.0, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    conn_max_retry_delay: float = Field(default=30.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    conn_disconnect_idle_timeout: int = Field(default=300, description=Msg.DISCONNECT_IDLE_TIMEOUT_DESCRIPTION)

    # Operation Retry settings
    op_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    op_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    op_initial_retry_delay: float = Field(default=0.5, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    op_max_retry_delay: float = Field(default=10.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    op_jitter: float = Field(default=0.1, description=Msg.JITTER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="MONGODB_")


class OracleSettings(_BaseDBSettings):
    """Oracle database settings with environment variable fallback."""

    host: str = Field(default="localhost", description=Msg.HOST_DESCRIPTION)
    port: int = Field(default=1521, description=Msg.PORT_DESCRIPTION)
    user: str = Field(default="system", description=Msg.USERNAME_DESCRIPTION)
    password: str = Field(default="oracle", description=Msg.PASSWORD_DESCRIPTION)
    servicename: str = Field(default="xe", description=Msg.SERVICE_NAME_DESCRIPTION)

    echo: bool = Field(default=False, description=Msg.ECHO_DESCRIPTION)
    autoflush: bool = Field(default=False, description=Msg.AUTOFLUSH_DESCRIPTION)
    expire_on_commit: bool = Field(default=False, description=Msg.EXPIRE_ON_COMMIT_DESCRIPTION)
    autocommit: bool = Field(default=True, description=Msg.AUTOCOMMIT_DESCRIPTION)
    connection_timeout: int = Field(default=30, description=Msg.CONNECTION_TIMEOUT_DESCRIPTION)
    pool_recycle: int = Field(default=3600, description=Msg.POOL_RECYCLE_DESCRIPTION)
    pool_size: int = Field(default=10, description=Msg.POOL_SIZE_DESCRIPTION)
    max_overflow: int = Field(default=20, description=Msg.MAX_OVERFLOW_DESCRIPTION)
    sync_driver: str = Field(default="oracle+oracledb", description=Msg.ORACLE_DRIVER_DESCRIPTION)

    # SSL settings
    ssl_enabled: bool = Field(default=False, description=Msg.SSL_ENABLED_DESCRIPTION)
    ssl_wallet_path: str | None = Field(default=None, description=Msg.SSL_WALLET_PATH_DESCRIPTION)

    # Connection Retry settings
    conn_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    conn_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    conn_initial_retry_delay: float = Field(default=1.0, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    conn_max_retry_delay: float = Field(default=30.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    conn_disconnect_idle_timeout: int = Field(default=300, description=Msg.DISCONNECT_IDLE_TIMEOUT_DESCRIPTION)

    # Operation Retry settings
    op_enable_retry: bool = Field(default=True, description=Msg.ENABLE_RETRY_DESCRIPTION)
    op_max_retries: int = Field(default=3, description=Msg.MAX_RETRIES_DESCRIPTION)
    op_initial_retry_delay: float = Field(default=0.5, description=Msg.INITIAL_RETRY_DELAY_DESCRIPTION)
    op_max_retry_delay: float = Field(default=10.0, description=Msg.MAX_RETRY_DELAY_DESCRIPTION)
    op_jitter: float = Field(default=0.1, description=Msg.JITTER_DESCRIPTION)

    model_config = SettingsConfigDict(env_prefix="ORACLE_")


# Create optimized cached getter functions using the factory
get_sqlite_settings = _create_cached_settings_factory(SQLiteSettings)
get_postgresql_settings = _create_cached_settings_factory(PostgreSQLSettings)
get_mssql_settings = _create_cached_settings_factory(MSSQLSettings)
get_mysql_settings = _create_cached_settings_factory(MySQLSettings)
get_mongodb_settings = _create_cached_settings_factory(MongoDBSettings)
get_oracle_settings = _create_cached_settings_factory(OracleSettings)
