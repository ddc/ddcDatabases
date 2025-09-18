from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import AsyncGenerator, Generator
from sqlalchemy import URL
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from .db_utils import BaseConnection
from .settings import get_postgresql_settings


@dataclass(slots=True, frozen=True)
class ConnectionConfig:
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None


@dataclass(slots=True, frozen=True)
class PoolConfig:
    pool_size: int | None = None
    max_overflow: int | None = None
    pool_recycle: int | None = None
    connection_timeout: int | None = None


@dataclass(slots=True, frozen=True)
class SessionConfig:
    echo: bool | None = None
    autoflush: bool | None = None
    expire_on_commit: bool | None = None
    autocommit: bool | None = None


class PostgreSQL(BaseConnection):
    """
    Class to handle PostgreSQL connections.
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
        'async_driver',
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
        database: str | None = None,
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
        _settings = get_postgresql_settings()

        # Create configuration objects using dataclasses
        self._connection_config = ConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
        )

        self._pool_config = PoolConfig(
            pool_size=pool_size or _settings.pool_size,
            max_overflow=max_overflow or _settings.max_overflow,
            pool_recycle=pool_recycle or _settings.pool_recycle,
            connection_timeout=connection_timeout or _settings.connection_timeout,
        )

        self._session_config = SessionConfig(
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
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver

        self.connection_url = {
            "host": self._connection_config.host,
            "port": self._connection_config.port,
            "database": self._connection_config.database,
            "username": self._connection_config.user,
            "password": self._connection_config.password,
        }

        self.extra_engine_args = extra_engine_args or {}
        self.engine_args = {
            "echo": self.echo,
            "pool_pre_ping": True,
            "pool_recycle": self.pool_recycle,
            **self.extra_engine_args,
        }

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self.autoflush,
            expire_on_commit=self.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
        )

    def __repr__(self) -> str:
        """String representation using configuration objects."""
        return (
            "PostgreSQL("
            f"host={self._connection_config.host!r}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database!r}, "
            f"pool_size={self._pool_config.pool_size}, "
            f"echo={self._session_config.echo}"
            ")"
        )

    def get_connection_info(self) -> ConnectionConfig:
        """Get immutable connection configuration."""
        return self._connection_config

    def get_pool_info(self) -> PoolConfig:
        """Get immutable pool configuration."""
        return self._pool_config

    def get_session_info(self) -> SessionConfig:
        """Get immutable session configuration."""
        return self._session_config

    def _get_base_engine_args(self, connection_url: URL, driver_connect_args: dict, driver_engine_args: dict) -> dict:
        existing_connect_args = self.engine_args.get("connect_args", {})
        merged_connect_args = {**existing_connect_args, **driver_connect_args}

        base_args = {
            "url": connection_url,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_pre_ping": True,
            "pool_recycle": self.pool_recycle,
            "query_cache_size": 1000,
            "connect_args": merged_connect_args,
            **{k: v for k, v in self.engine_args.items() if k != "connect_args"},
        }

        # Add driver-specific engine arguments
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

        if "psycopg2" in self.sync_driver:
            sync_connect_args["connect_timeout"] = self.connection_timeout
            if self.autocommit:
                sync_engine_args["isolation_level"] = "AUTOCOMMIT"

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
            async_connect_args["command_timeout"] = self.connection_timeout
            if self.autocommit:
                async_engine_args["isolation_level"] = "AUTOCOMMIT"

        _engine_args = self._get_base_engine_args(_connection_url, async_connect_args, async_engine_args)
        _engine = create_async_engine(**_engine_args)
        yield _engine
        await _engine.dispose()
