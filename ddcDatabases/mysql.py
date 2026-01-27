from .core.base import BaseConnection
from .core.configs import (
    BaseConnectionConfig,
    BaseOperationRetryConfig,
    BasePoolConfig,
    BaseRetryConfig,
    BaseSessionConfig,
    BaseSSLConfig,
)
from .core.constants import MYSQL_SSL_MODES
from .core.settings import get_mysql_settings
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import logging
from sqlalchemy.engine import URL, Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from typing import Any, AsyncGenerator, Generator

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True, slots=True)
class MySQLConnectionConfig(BaseConnectionConfig):
    database: str | None = None


@dataclass(frozen=True, slots=True)
class MySQLSSLConfig(BaseSSLConfig):
    def __post_init__(self) -> None:
        if self.ssl_mode is not None and self.ssl_mode.upper() not in MYSQL_SSL_MODES:
            raise ValueError(f"ssl_mode must be one of {sorted(MYSQL_SSL_MODES)}, got '{self.ssl_mode}'")


@dataclass(frozen=True, slots=True)
class MySQLPoolConfig(BasePoolConfig):
    pass


@dataclass(frozen=True, slots=True)
class MySQLSessionConfig(BaseSessionConfig):
    pass


@dataclass(frozen=True, slots=True)
class MySQLConnRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class MySQLOpRetryConfig(BaseOperationRetryConfig):
    pass


class MySQL(BaseConnection):
    """
    Class to handle MySQL connections.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        pool_config: MySQLPoolConfig | None = None,
        session_config: MySQLSessionConfig | None = None,
        conn_retry_config: MySQLConnRetryConfig | None = None,
        op_retry_config: MySQLOpRetryConfig | None = None,
        ssl_config: MySQLSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        _settings = get_mysql_settings()

        # Normalize localhost to 127.0.0.1 to force TCP connection
        # mysqlclient uses Unix socket when host is 'localhost'
        _host = host or _settings.host
        if _host and _host.lower() == "localhost":
            _host = "127.0.0.1"

        self._connection_config = MySQLConnectionConfig(
            host=_host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
        )

        _pc = pool_config or MySQLPoolConfig()
        self._pool_config = MySQLPoolConfig(
            pool_size=_pc.pool_size if _pc.pool_size is not None else _settings.pool_size,
            max_overflow=_pc.max_overflow if _pc.max_overflow is not None else _settings.max_overflow,
            pool_recycle=_pc.pool_recycle if _pc.pool_recycle is not None else _settings.pool_recycle,
            connection_timeout=(
                _pc.connection_timeout if _pc.connection_timeout is not None else _settings.connection_timeout
            ),
        )

        _sc = session_config or MySQLSessionConfig()
        self._session_config = MySQLSessionConfig(
            echo=_sc.echo if _sc.echo is not None else _settings.echo,
            autoflush=_sc.autoflush if _sc.autoflush is not None else _settings.autoflush,
            expire_on_commit=_sc.expire_on_commit if _sc.expire_on_commit is not None else _settings.expire_on_commit,
            autocommit=_sc.autocommit if _sc.autocommit is not None else _settings.autocommit,
        )

        _ssl = ssl_config or MySQLSSLConfig()
        self._ssl_config = MySQLSSLConfig(
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
            "username": self._connection_config.user,
            "password": self._connection_config.password,
            "database": self._connection_config.database,
        }

        self.extra_engine_args = extra_engine_args or {}
        _connect_args: dict[str, Any] = {
            "charset": "utf8mb4",
            "autocommit": self._session_config.autocommit,
            "connect_timeout": self._pool_config.connection_timeout,
        }
        if self._ssl_config.ssl_mode and self._ssl_config.ssl_mode != "DISABLED":
            ssl_dict = {}
            if self._ssl_config.ssl_ca_cert_path:
                ssl_dict["ca"] = self._ssl_config.ssl_ca_cert_path
            if self._ssl_config.ssl_client_cert_path:
                ssl_dict["cert"] = self._ssl_config.ssl_client_cert_path
            if self._ssl_config.ssl_client_key_path:
                ssl_dict["key"] = self._ssl_config.ssl_client_key_path
            _connect_args["ssl"] = ssl_dict
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
        _crc = conn_retry_config or MySQLConnRetryConfig()
        self._conn_retry_config = MySQLConnRetryConfig(
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
        _orc = op_retry_config or MySQLOpRetryConfig()
        self._op_retry_config = MySQLOpRetryConfig(
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
            async_driver=self.async_driver,
            conn_retry_config=self._conn_retry_config,
            op_retry_config=self._op_retry_config,
            logger=self.logger,
        )

        self.logger.debug(
            f"Initializing MySQL(host={self._connection_config.host}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database})"
        )

    def __repr__(self) -> str:
        return (
            "MySQL("
            f"host={self._connection_config.host!r}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database!r}, "
            f"pool_size={self._pool_config.pool_size}, "
            f"echo={self._session_config.echo}"
            ")"
        )

    def get_connection_info(self) -> MySQLConnectionConfig:
        return self._connection_config

    def get_pool_info(self) -> MySQLPoolConfig:
        return self._pool_config

    def get_session_info(self) -> MySQLSessionConfig:
        return self._session_config

    def get_conn_retry_info(self) -> MySQLConnRetryConfig:
        return self._conn_retry_config

    def get_op_retry_info(self) -> MySQLOpRetryConfig:
        return self._op_retry_config

    def get_ssl_info(self) -> MySQLSSLConfig:
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
        _connection_url = URL.create(
            drivername=self.async_driver,
            **self.connection_url,
        )
        _engine_args = {
            "url": _connection_url,
            **self.engine_args,
        }
        _engine = create_async_engine(**_engine_args)
        yield _engine
        await _engine.dispose()
