import logging
import ssl as _ssl_module
from .core.base import BaseConnection
from .core.configs import (
    CONNECTION_RETRY_FIELD_MAP,
    OPERATION_RETRY_FIELD_MAP,
    BaseConnectionConfig,
    BaseOperationRetryConfig,
    BasePoolConfig,
    BaseRetryConfig,
    BaseSessionConfig,
    BaseSSLConfig,
    merge_config_with_settings,
)
from .core.constants import POSTGRESQL_SSL_MODES
from .core.settings import get_postgresql_settings
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from sqlalchemy import URL
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True, slots=True)
class PostgreSQLConnectionConfig(BaseConnectionConfig):
    database: str | None = None
    schema: str | None = None


@dataclass(frozen=True, slots=True)
class PostgreSQLSSLConfig(BaseSSLConfig):
    def __post_init__(self) -> None:
        if self.ssl_mode is not None and self.ssl_mode.lower() not in POSTGRESQL_SSL_MODES:
            raise ValueError(f"ssl_mode must be one of {sorted(POSTGRESQL_SSL_MODES)}, got '{self.ssl_mode}'")


@dataclass(frozen=True, slots=True)
class PostgreSQLPoolConfig(BasePoolConfig):
    pass


@dataclass(frozen=True, slots=True)
class PostgreSQLSessionConfig(BaseSessionConfig):
    pass


@dataclass(frozen=True, slots=True)
class PostgreSQLConnectionRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class PostgreSQLOperationRetryConfig(BaseOperationRetryConfig):
    pass


class PostgreSQL(BaseConnection):
    """
    Class to handle PostgreSQL connections.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        pool_config: PostgreSQLPoolConfig | None = None,
        session_config: PostgreSQLSessionConfig | None = None,
        connection_retry_config: PostgreSQLConnectionRetryConfig | None = None,
        operation_retry_config: PostgreSQLOperationRetryConfig | None = None,
        ssl_config: PostgreSQLSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: Any = None,
    ) -> None:
        _settings = get_postgresql_settings()

        self._connection_config = PostgreSQLConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
            schema=schema if schema is not None else _settings.schema,
        )

        self._pool_config = merge_config_with_settings(PostgreSQLPoolConfig, pool_config, _settings)
        self._session_config = merge_config_with_settings(PostgreSQLSessionConfig, session_config, _settings)

        _ssl = ssl_config or PostgreSQLSSLConfig()
        self._ssl_config = PostgreSQLSSLConfig(
            ssl_mode=_ssl.ssl_mode if _ssl.ssl_mode is not None else _settings.ssl_mode,
            ssl_ca_cert_path=_ssl.ssl_ca_cert_path if _ssl.ssl_ca_cert_path is not None else _settings.ssl_ca_cert_path,
            ssl_client_cert_path=(
                _ssl.ssl_client_cert_path if _ssl.ssl_client_cert_path is not None else _settings.ssl_client_cert_path
            ),
            ssl_client_key_path=(
                _ssl.ssl_client_key_path if _ssl.ssl_client_key_path is not None else _settings.ssl_client_key_path
            ),
        )

        self.sync_driver = _settings.sync_driver
        self.async_driver = _settings.async_driver

        self.connection_url = {
            "host": self._connection_config.host,
            "port": self._connection_config.port,
            "database": self._connection_config.database,
            "username": self._connection_config.user,
            "password": self._connection_config.password,
        }

        self.extra_engine_args = extra_engine_args or {}
        self.engine_args = {
            "echo": self._session_config.echo,
            "pool_pre_ping": True,
            "pool_recycle": self._pool_config.pool_recycle,
            **self.extra_engine_args,
        }

        self._connection_retry_config = merge_config_with_settings(
            PostgreSQLConnectionRetryConfig, connection_retry_config, _settings, CONNECTION_RETRY_FIELD_MAP
        )
        self._operation_retry_config = merge_config_with_settings(
            PostgreSQLOperationRetryConfig, operation_retry_config, _settings, OPERATION_RETRY_FIELD_MAP
        )

        self.logger = logger if logger is not None else _logger

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self._session_config.autoflush,
            expire_on_commit=self._session_config.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
            connection_retry_config=self._connection_retry_config,
            operation_retry_config=self._operation_retry_config,
            logger=self.logger,
        )

        self.logger.debug(
            f"Initializing PostgreSQL(host={self._connection_config.host}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database})"
        )

    def __repr__(self) -> str:
        return (
            "PostgreSQL("
            f"host={self._connection_config.host!r}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database!r}, "
            f"pool_size={self._pool_config.pool_size}, "
            f"echo={self._session_config.echo}"
            ")"
        )

    def get_connection_info(self) -> PostgreSQLConnectionConfig:
        return self._connection_config

    def get_pool_info(self) -> PostgreSQLPoolConfig:
        return self._pool_config

    def get_session_info(self) -> PostgreSQLSessionConfig:
        return self._session_config

    def get_connection_retry_info(self) -> PostgreSQLConnectionRetryConfig:
        return self._connection_retry_config

    def get_operation_retry_info(self) -> PostgreSQLOperationRetryConfig:
        return self._operation_retry_config

    def get_ssl_info(self) -> PostgreSQLSSLConfig:
        return self._ssl_config

    def _get_base_engine_args(self, connection_url: URL, driver_connect_args: dict, driver_engine_args: dict) -> dict:
        existing_connect_args = self.engine_args.get("connect_args", {})
        merged_connect_args = {**existing_connect_args, **driver_connect_args}

        base_args = {
            "url": connection_url,
            "pool_size": self._pool_config.pool_size,
            "max_overflow": self._pool_config.max_overflow,
            "pool_pre_ping": True,
            "pool_recycle": self._pool_config.pool_recycle,
            "query_cache_size": 1000,
            "connect_args": merged_connect_args,
            **{k: v for k, v in self.engine_args.items() if k != "connect_args"},
        }

        base_args.update(driver_engine_args)
        return base_args

    @contextmanager
    def _get_engine(self) -> Generator[Engine, None, None]:
        _connection_url = URL.create(
            drivername=self.sync_driver,
            **self.connection_url,
        )

        sync_connect_args = {}
        sync_engine_args = {}

        if "psycopg" in self.sync_driver:
            sync_connect_args["connect_timeout"] = self._pool_config.connection_timeout
            if self._session_config.autocommit:
                sync_engine_args["isolation_level"] = "AUTOCOMMIT"
            if self._connection_config.schema and self._connection_config.schema != "public":
                sync_connect_args["options"] = f"-c search_path={self._connection_config.schema}"
            if self._ssl_config.ssl_mode and self._ssl_config.ssl_mode != "disable":
                sync_connect_args["sslmode"] = self._ssl_config.ssl_mode
                if self._ssl_config.ssl_ca_cert_path:
                    sync_connect_args["sslrootcert"] = self._ssl_config.ssl_ca_cert_path
                if self._ssl_config.ssl_client_cert_path:
                    sync_connect_args["sslcert"] = self._ssl_config.ssl_client_cert_path
                if self._ssl_config.ssl_client_key_path:
                    sync_connect_args["sslkey"] = self._ssl_config.ssl_client_key_path

        _engine_args = self._get_base_engine_args(_connection_url, sync_connect_args, sync_engine_args)
        _engine = create_engine(**_engine_args)
        yield _engine
        _engine.dispose()

    @asynccontextmanager
    async def _get_async_engine(self) -> AsyncGenerator[AsyncEngine, None]:
        _connection_url = URL.create(
            drivername=self.async_driver,
            **self.connection_url,
        )

        async_connect_args = {}
        async_engine_args = {}

        if "asyncpg" in self.async_driver:
            async_connect_args["command_timeout"] = self._pool_config.connection_timeout
            if self._session_config.autocommit:
                async_engine_args["isolation_level"] = "AUTOCOMMIT"
            if self._connection_config.schema and self._connection_config.schema != "public":
                async_connect_args["server_settings"] = {"search_path": self._connection_config.schema}
            if self._ssl_config.ssl_mode and self._ssl_config.ssl_mode != "disable":
                if self._ssl_config.ssl_ca_cert_path:
                    ssl_context = _ssl_module.SSLContext(_ssl_module.PROTOCOL_TLS_CLIENT)
                    ssl_context.minimum_version = _ssl_module.TLSVersion.TLSv1_3
                    ssl_context.load_verify_locations(cafile=self._ssl_config.ssl_ca_cert_path)
                    if self._ssl_config.ssl_client_cert_path and self._ssl_config.ssl_client_key_path:
                        ssl_context.load_cert_chain(
                            certfile=self._ssl_config.ssl_client_cert_path,
                            keyfile=self._ssl_config.ssl_client_key_path,
                        )
                    async_connect_args["ssl"] = ssl_context
                else:
                    async_connect_args["ssl"] = self._ssl_config.ssl_mode

        _engine_args = self._get_base_engine_args(_connection_url, async_connect_args, async_engine_args)
        _engine = create_async_engine(**_engine_args)
        yield _engine
        await _engine.dispose()
