import logging
from .core.base import BaseConnection
from .core.configs import (
    CONNECTION_RETRY_FIELD_MAP,
    OPERATION_RETRY_FIELD_MAP,
    BaseConnectionConfig,
    BaseOperationRetryConfig,
    BasePoolConfig,
    BaseRetryConfig,
    BaseSessionConfig,
    merge_config_with_settings,
)
from .core.settings import get_oracle_settings
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from sqlalchemy.ext.asyncio import AsyncEngine
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True, slots=True)
class OracleConnectionConfig(BaseConnectionConfig):
    servicename: str | None = None


@dataclass(frozen=True, slots=True)
class OracleSSLConfig:
    ssl_enabled: bool | None = None
    ssl_wallet_path: str | None = None


@dataclass(frozen=True, slots=True)
class OraclePoolConfig(BasePoolConfig):
    pass


@dataclass(frozen=True, slots=True)
class OracleSessionConfig(BaseSessionConfig):
    pass


@dataclass(frozen=True, slots=True)
class OracleConnectionRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class OracleOperationRetryConfig(BaseOperationRetryConfig):
    pass


class Oracle(BaseConnection):
    """
    Class to handle Oracle connections.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        servicename: str | None = None,
        pool_config: OraclePoolConfig | None = None,
        session_config: OracleSessionConfig | None = None,
        connection_retry_config: OracleConnectionRetryConfig | None = None,
        operation_retry_config: OracleOperationRetryConfig | None = None,
        ssl_config: OracleSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: Any = None,
    ) -> None:
        _settings = get_oracle_settings()

        self._connection_config = OracleConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            servicename=servicename or _settings.servicename,
        )

        self._pool_config = merge_config_with_settings(OraclePoolConfig, pool_config, _settings)
        self._session_config = merge_config_with_settings(OracleSessionConfig, session_config, _settings)

        _ssl = ssl_config or OracleSSLConfig()
        self._ssl_config = OracleSSLConfig(
            ssl_enabled=_ssl.ssl_enabled if _ssl.ssl_enabled is not None else _settings.ssl_enabled,
            ssl_wallet_path=_ssl.ssl_wallet_path if _ssl.ssl_wallet_path is not None else _settings.ssl_wallet_path,
        )

        self.sync_driver = _settings.sync_driver

        self.connection_url = {
            "host": self._connection_config.host,
            "port": self._connection_config.port,
            "username": self._connection_config.user,
            "password": self._connection_config.password,
            "query": {
                "service_name": self._connection_config.servicename,
            },
        }

        self.extra_engine_args = extra_engine_args or {}
        _connect_args = {}
        if self._ssl_config.ssl_wallet_path:
            _connect_args["wallet_location"] = self._ssl_config.ssl_wallet_path
        self.engine_args = {
            "echo": self._session_config.echo,
            "pool_pre_ping": True,
            "pool_recycle": self._pool_config.pool_recycle,
            "pool_size": self._pool_config.pool_size,
            "max_overflow": self._pool_config.max_overflow,
            "connect_args": _connect_args,
            **self.extra_engine_args,
        }

        self._connection_retry_config = merge_config_with_settings(
            OracleConnectionRetryConfig, connection_retry_config, _settings, CONNECTION_RETRY_FIELD_MAP
        )
        self._operation_retry_config = merge_config_with_settings(
            OracleOperationRetryConfig, operation_retry_config, _settings, OPERATION_RETRY_FIELD_MAP
        )

        self.logger = logger if logger is not None else _logger

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self._session_config.autoflush,
            expire_on_commit=self._session_config.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=None,
            connection_retry_config=self._connection_retry_config,
            operation_retry_config=self._operation_retry_config,
            logger=self.logger,
        )

        self.logger.debug(
            f"Initializing Oracle(host={self._connection_config.host}, "
            f"port={self._connection_config.port}, "
            f"servicename={self._connection_config.servicename})"
        )

    def __repr__(self) -> str:
        return (
            "Oracle("
            f"host={self._connection_config.host!r}, "
            f"port={self._connection_config.port}, "
            f"servicename={self._connection_config.servicename!r}, "
            f"pool_size={self._pool_config.pool_size}, "
            f"echo={self._session_config.echo}"
            ")"
        )

    def get_connection_info(self) -> OracleConnectionConfig:
        return self._connection_config

    def get_pool_info(self) -> OraclePoolConfig:
        return self._pool_config

    def get_session_info(self) -> OracleSessionConfig:
        return self._session_config

    def get_connection_retry_info(self) -> OracleConnectionRetryConfig:
        return self._connection_retry_config

    def get_operation_retry_info(self) -> OracleOperationRetryConfig:
        return self._operation_retry_config

    def get_ssl_info(self) -> OracleSSLConfig:
        return self._ssl_config

    @asynccontextmanager
    async def _get_async_engine(self) -> AsyncGenerator[AsyncEngine, None]:
        raise NotImplementedError("Oracle doesn't support async operations. Use synchronous methods only.")
