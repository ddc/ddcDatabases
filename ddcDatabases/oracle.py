from .core.base import BaseConnection
from .core.configs import BaseConnectionConfig, PoolConfig, RetryConfig, SessionConfig
from .core.retry import RetryPolicy
from .core.settings import get_oracle_settings
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import logging
from sqlalchemy.engine import URL, Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine
from typing import AsyncGenerator, Generator

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True)
class OracleConnectionConfig(BaseConnectionConfig):
    servicename: str | None = None


@dataclass(frozen=True)
class OracleSSLConfig:
    ssl_enabled: bool | None = None
    ssl_wallet_path: str | None = None


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
        pool_config: PoolConfig | None = None,
        session_config: SessionConfig | None = None,
        retry_config: RetryConfig | None = None,
        ssl_config: OracleSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: logging.Logger | None = None,
    ):
        _settings = get_oracle_settings()

        self._connection_config = OracleConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            servicename=servicename or _settings.servicename,
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
            async_driver=None,
            retry_config=_retry_policy,
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

    def get_pool_info(self) -> PoolConfig:
        return self._pool_config

    def get_session_info(self) -> SessionConfig:
        return self._session_config

    def get_retry_info(self) -> RetryConfig:
        return self._retry_config

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
