from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator
from sqlalchemy import URL
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from .db_utils import BaseConnection
from .settings import get_postgresql_settings


class PostgreSQL(BaseConnection):
    """
    Class to handle PostgreSQL connections
    """

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

        self.echo = echo if echo is not None else _settings.echo
        self.autoflush = autoflush if autoflush is not None else _settings.autoflush
        self.expire_on_commit = expire_on_commit if expire_on_commit is not None else _settings.expire_on_commit
        self.autocommit = autocommit if autocommit is not None else _settings.autocommit
        self.connection_timeout = connection_timeout or _settings.connection_timeout
        self.pool_recycle = pool_recycle or _settings.pool_recycle
        self.pool_size = pool_size or _settings.pool_size
        self.max_overflow = max_overflow or _settings.max_overflow
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.connection_url = {
            "host": host or _settings.host,
            "port": int(port or _settings.port),
            "database": database or _settings.database,
            "username": user or _settings.user,
            "password": password or _settings.password,
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

    def _get_base_engine_args(self, connection_url: URL, driver_connect_args: dict, driver_engine_args: dict) -> dict:
        """Get base engine arguments with driver-specific connect_args and engine args."""
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
