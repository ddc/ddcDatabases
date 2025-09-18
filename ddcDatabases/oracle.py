from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import AsyncGenerator, Generator
from sqlalchemy.engine import create_engine, Engine, URL
from sqlalchemy.ext.asyncio import AsyncEngine
from .db_utils import BaseConnection
from .settings import get_oracle_settings


@dataclass(slots=True, frozen=True)
class OracleConnectionConfig:
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    servicename: str | None = None


@dataclass(slots=True, frozen=True)
class OraclePoolConfig:
    pool_size: int | None = None
    max_overflow: int | None = None
    pool_recycle: int | None = None
    connection_timeout: int | None = None


@dataclass(slots=True, frozen=True)
class OracleSessionConfig:
    echo: bool | None = None
    autoflush: bool | None = None
    expire_on_commit: bool | None = None
    autocommit: bool | None = None


class Oracle(BaseConnection):
    """
    Class to handle Oracle connections.
    """

    __slots__ = (
        'echo',
        'autoflush',
        'expire_on_commit',
        'autocommit',
        'connection_timeout',
        'pool_recycle',
        'pool_size',
        'max_overflow',
        'sync_driver',
        'connection_url',
        'extra_engine_args',
        'engine_args',
        '_connection_config',
        '_pool_config',
        '_session_config',
    )

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        servicename: str | None = None,
        echo: bool | None = None,
        autoflush: bool | None = None,
        expire_on_commit: bool | None = None,
        autocommit: bool | None = None,
        connection_timeout: int | None = None,
        pool_recycle: int | None = None,
        pool_size: int | None = None,
        max_overflow: int | None = None,
        extra_engine_args: dict | None = None,
    ):
        _settings = get_oracle_settings()

        # Create configuration objects using dataclasses
        self._connection_config = OracleConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            servicename=servicename or _settings.servicename,
        )

        self._pool_config = OraclePoolConfig(
            pool_size=pool_size or _settings.pool_size,
            max_overflow=max_overflow or _settings.max_overflow,
            pool_recycle=pool_recycle or _settings.pool_recycle,
            connection_timeout=connection_timeout or _settings.connection_timeout,
        )

        self._session_config = OracleSessionConfig(
            echo=echo if echo is not None else _settings.echo,
            autoflush=autoflush if autoflush is not None else _settings.autoflush,
            expire_on_commit=expire_on_commit if expire_on_commit is not None else _settings.expire_on_commit,
            autocommit=autocommit if autocommit is not None else _settings.autocommit,
        )

        # Set instance attributes for backward compatibility
        self.echo = self._session_config.echo
        self.autoflush = self._session_config.autoflush
        self.expire_on_commit = self._session_config.expire_on_commit
        self.autocommit = self._session_config.autocommit
        self.connection_timeout = self._pool_config.connection_timeout
        self.pool_recycle = self._pool_config.pool_recycle
        self.pool_size = self._pool_config.pool_size
        self.max_overflow = self._pool_config.max_overflow
        self.sync_driver = _settings.sync_driver

        self.connection_url = {
            "host": self._connection_config.host,
            "port": self._connection_config.port,
            "username": self._connection_config.user,
            "password": self._connection_config.password,
            "query": {
                "service_name": self._connection_config.servicename,
                "encoding": "UTF-8",
                "nencoding": "UTF-8",
            },
        }

        self.extra_engine_args = extra_engine_args or {}
        self.engine_args = {
            "echo": self.echo,
            "pool_pre_ping": True,
            "pool_recycle": self.pool_recycle,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "connect_args": {
                "threaded": True,
                "events": True,
                "autocommit": self.autocommit,
            },
            **self.extra_engine_args,
        }

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self.autoflush,
            expire_on_commit=self.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=None,
        )

    def __repr__(self) -> str:
        """String representation using configuration objects."""
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
        """Get immutable connection configuration."""
        return self._connection_config

    def get_pool_info(self) -> OraclePoolConfig:
        """Get immutable pool configuration."""
        return self._pool_config

    def get_session_info(self) -> OracleSessionConfig:
        """Get immutable session configuration."""
        return self._session_config

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
