from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import AsyncGenerator, Generator
from sqlalchemy.engine import create_engine, Engine, URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session
from .db_utils import BaseConnection, ConnectionTester
from .settings import get_mssql_settings


@dataclass(slots=True, frozen=True)
class MSSQLConnectionConfig:
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None
    schema: str | None = None
    odbcdriver_version: int | None = None


@dataclass(slots=True, frozen=True)
class MSSQLPoolConfig:
    pool_size: int | None = None
    max_overflow: int | None = None
    pool_recycle: int | None = None
    connection_timeout: int | None = None


@dataclass(slots=True, frozen=True)
class MSSQLSessionConfig:
    echo: bool | None = None
    autoflush: bool | None = None
    expire_on_commit: bool | None = None
    autocommit: bool | None = None


class MSSQL(BaseConnection):
    """
    Class to handle MSSQL connections.
    """

    __slots__ = (
        'schema',
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
        'odbcdriver_version',
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
        schema: str | None = None,
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
        _settings = get_mssql_settings()

        # Create configuration objects using dataclasses
        self._connection_config = MSSQLConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
            schema=schema or _settings.db_schema,
            odbcdriver_version=int(_settings.odbcdriver_version),
        )

        self._pool_config = MSSQLPoolConfig(
            pool_size=pool_size or int(_settings.pool_size),
            max_overflow=max_overflow or int(_settings.max_overflow),
            pool_recycle=pool_recycle or _settings.pool_recycle,
            connection_timeout=connection_timeout or _settings.connection_timeout,
        )

        self._session_config = MSSQLSessionConfig(
            echo=echo if echo is not None else _settings.echo,
            autoflush=autoflush if autoflush is not None else _settings.autoflush,
            expire_on_commit=expire_on_commit if expire_on_commit is not None else _settings.expire_on_commit,
            autocommit=autocommit if autocommit is not None else _settings.autocommit,
        )

        # Set instance attributes for backward compatibility
        self.schema = self._connection_config.schema
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
        self.odbcdriver_version = self._connection_config.odbcdriver_version

        self.connection_url = {
            "host": self._connection_config.host,
            "port": self._connection_config.port,
            "database": self._connection_config.database,
            "username": self._connection_config.user,
            "password": self._connection_config.password,
            "query": {
                "driver": f"ODBC Driver {self.odbcdriver_version} for SQL Server",
                "TrustServerCertificate": "yes",
            },
        }

        self.extra_engine_args = extra_engine_args or {}
        self.engine_args = {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "echo": self.echo,
            "pool_pre_ping": True,
            "pool_recycle": self.pool_recycle,
            "connect_args": {
                "timeout": self.connection_timeout,
                "login_timeout": self.connection_timeout,
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
            async_driver=self.async_driver,
        )

    def __repr__(self) -> str:
        """String representation using configuration objects."""
        return (
            "MSSQL("
            f"host={self._connection_config.host!r}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database!r}, "
            f"schema={self._connection_config.schema!r}, "
            f"pool_size={self._pool_config.pool_size}, "
            f"echo={self._session_config.echo}"
            ")"
        )

    def get_connection_info(self) -> MSSQLConnectionConfig:
        """Get immutable connection configuration."""
        return self._connection_config

    def get_pool_info(self) -> MSSQLPoolConfig:
        """Get immutable pool configuration."""
        return self._pool_config

    def get_session_info(self) -> MSSQLSessionConfig:
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
        _engine.update_execution_options(schema_translate_map={None: self.schema})
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
        _engine.update_execution_options(schema_translate_map={None: self.schema})
        yield _engine
        await _engine.dispose()

    def _test_connection_sync(self, session: Session) -> None:
        del self.connection_url["password"]
        del self.connection_url["query"]
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.sync_driver,
            query={"schema": self.schema},
        )
        test_connection = ConnectionTester(
            sync_session=session,
            host_url=_connection_url,
        )
        test_connection.test_connection_sync()

    async def _test_connection_async(self, session: AsyncSession) -> None:
        del self.connection_url["password"]
        del self.connection_url["query"]
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.async_driver,
            query={"schema": self.schema},
        )
        test_connection = ConnectionTester(
            async_session=session,
            host_url=_connection_url,
        )
        await test_connection.test_connection_async()
