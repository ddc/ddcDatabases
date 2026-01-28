import logging
from .core.base import BaseConnection
from .core.configs import (
    BaseConnectionConfig,
    BaseOperationRetryConfig,
    BasePoolConfig,
    BaseRetryConfig,
    BaseSessionConfig,
)
from .core.settings import get_oracle_settings
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from sqlalchemy.engine import URL, Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine
from typing import AsyncGenerator, Generator

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
class OracleConnRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class OracleOpRetryConfig(BaseOperationRetryConfig):
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
        conn_retry_config: OracleConnRetryConfig | None = None,
        op_retry_config: OracleOpRetryConfig | None = None,
        ssl_config: OracleSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        _settings = get_oracle_settings()

        self._connection_config = OracleConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            servicename=servicename or _settings.servicename,
        )

        _pc = pool_config or OraclePoolConfig()
        self._pool_config = OraclePoolConfig(
            pool_size=_pc.pool_size if _pc.pool_size is not None else _settings.pool_size,
            max_overflow=_pc.max_overflow if _pc.max_overflow is not None else _settings.max_overflow,
            pool_recycle=_pc.pool_recycle if _pc.pool_recycle is not None else _settings.pool_recycle,
            connection_timeout=(
                _pc.connection_timeout if _pc.connection_timeout is not None else _settings.connection_timeout
            ),
        )

        _sc = session_config or OracleSessionConfig()
        self._session_config = OracleSessionConfig(
            echo=_sc.echo if _sc.echo is not None else _settings.echo,
            autoflush=_sc.autoflush if _sc.autoflush is not None else _settings.autoflush,
            expire_on_commit=_sc.expire_on_commit if _sc.expire_on_commit is not None else _settings.expire_on_commit,
            autocommit=_sc.autocommit if _sc.autocommit is not None else _settings.autocommit,
        )

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

        # Create connection retry configuration
        _crc = conn_retry_config or OracleConnRetryConfig()
        self._conn_retry_config = OracleConnRetryConfig(
            enable_retry=_crc.enable_retry if _crc.enable_retry is not None else _settings.conn_enable_retry,
            max_retries=_crc.max_retries if _crc.max_retries is not None else _settings.conn_max_retries,
            initial_retry_delay=(
                _crc.initial_retry_delay if _crc.initial_retry_delay is not None else _settings.conn_initial_retry_delay
            ),
            max_retry_delay=(
                _crc.max_retry_delay if _crc.max_retry_delay is not None else _settings.conn_max_retry_delay
            ),
        )

        # Create operation retry configuration
        _orc = op_retry_config or OracleOpRetryConfig()
        self._op_retry_config = OracleOpRetryConfig(
            enable_retry=_orc.enable_retry if _orc.enable_retry is not None else _settings.op_enable_retry,
            max_retries=_orc.max_retries if _orc.max_retries is not None else _settings.op_max_retries,
            initial_retry_delay=(
                _orc.initial_retry_delay if _orc.initial_retry_delay is not None else _settings.op_initial_retry_delay
            ),
            max_retry_delay=_orc.max_retry_delay if _orc.max_retry_delay is not None else _settings.op_max_retry_delay,
            jitter=_orc.jitter if _orc.jitter is not None else _settings.op_jitter,
        )

        self.logger = logger if logger is not None else _logger

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self._session_config.autoflush,
            expire_on_commit=self._session_config.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=None,
            conn_retry_config=self._conn_retry_config,
            op_retry_config=self._op_retry_config,
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

    def get_conn_retry_info(self) -> OracleConnRetryConfig:
        return self._conn_retry_config

    def get_op_retry_info(self) -> OracleOpRetryConfig:
        return self._op_retry_config

    def get_ssl_info(self) -> OracleSSLConfig:
        return self._ssl_config

    @contextmanager
    def _get_engine(self) -> Generator[Engine, None, None]:
        _connection_url = URL.create(
            drivername=self.sync_driver,
            **self.connection_url,
        )
        _engine_args = {
            "url": _connection_url,
            **self.engine_args,
        }
        _engine = create_engine(**_engine_args)
        yield _engine
        _engine.dispose()

    @asynccontextmanager
    async def _get_async_engine(self) -> AsyncGenerator[AsyncEngine, None]:
        raise NotImplementedError("Oracle doesn't support async operations. Use synchronous methods only.")
