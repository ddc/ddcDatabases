from .core.base import BaseConnection
from .core.configs import BaseConnectionConfig, BaseSSLConfig, PoolConfig, RetryConfig, SessionConfig
from .core.retry import RetryPolicy
from .core.settings import get_mysql_settings
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import logging
from sqlalchemy.engine import URL, Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from typing import Any, AsyncGenerator, Generator

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True)
class MySQLConnectionConfig(BaseConnectionConfig):
    database: str | None = None


@dataclass(frozen=True)
class MySQLSSLConfig(BaseSSLConfig):
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
        pool_config: PoolConfig | None = None,
        session_config: SessionConfig | None = None,
        retry_config: RetryConfig | None = None,
        ssl_config: MySQLSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: logging.Logger | None = None,
    ):
        _settings = get_mysql_settings()

        self._connection_config = MySQLConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
        )

        _pc = pool_config or PoolConfig()
        self._pool_config = PoolConfig(
            pool_size=_pc.pool_size if _pc.pool_size is not None else _settings.pool_size,
            max_overflow=_pc.max_overflow if _pc.max_overflow is not None else _settings.max_overflow,
            pool_recycle=_pc.pool_recycle if _pc.pool_recycle is not None else _settings.pool_recycle,
            connection_timeout=(
                _pc.connection_timeout if _pc.connection_timeout is not None else _settings.connection_timeout
            ),
        )

        _sc = session_config or SessionConfig()
        self._session_config = SessionConfig(
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

        # Create retry configuration
        _rc = retry_config or RetryConfig()
        self._retry_config = RetryConfig(
            enable_retry=_rc.enable_retry if _rc.enable_retry is not None else _settings.enable_retry,
            max_retries=_rc.max_retries if _rc.max_retries is not None else _settings.max_retries,
            initial_retry_delay=(
                _rc.initial_retry_delay if _rc.initial_retry_delay is not None else _settings.initial_retry_delay
            ),
            max_retry_delay=_rc.max_retry_delay if _rc.max_retry_delay is not None else _settings.max_retry_delay,
        )

        _retry_policy = RetryPolicy(
            enable_retry=self._retry_config.enable_retry,
            max_retries=self._retry_config.max_retries,
            initial_delay=self._retry_config.initial_retry_delay,
            max_delay=self._retry_config.max_retry_delay,
        )

        self.logger = logger if logger is not None else _logger

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self._session_config.autoflush,
            expire_on_commit=self._session_config.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
            retry_config=_retry_policy,
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

    def get_pool_info(self) -> PoolConfig:
        return self._pool_config

    def get_session_info(self) -> SessionConfig:
        return self._session_config

    def get_retry_info(self) -> RetryConfig:
        return self._retry_config

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
