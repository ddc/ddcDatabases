# -*- coding: utf-8 -*-
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Optional
from sqlalchemy.engine import create_engine, Engine, URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from .db_utils import TestConnections
from .settings import MSSQLSettings


class MSSQL:
    """
    Class to handle MSSQL connections
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        echo: Optional[bool] = None,
        pool_size: Optional[int] = None,
        max_overflow: Optional[int] = None,
    ):
        _settings = MSSQLSettings()
        self.host = host or _settings.host
        self.username = username or _settings.username
        self.password = password or _settings.password
        self.port = port or int(_settings.port)
        self.database = database or _settings.database
        self.schema = schema or _settings.db_schema
        self.echo = echo or _settings.echo
        self.pool_size = pool_size or int(_settings.pool_size)
        self.max_overflow = max_overflow or int(_settings.max_overflow)

        self.temp_engine: Optional[Engine | AsyncEngine] = None
        self.session: Optional[Session | AsyncSession] = None
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.odbcdriver_version = int(_settings.odbcdriver_version)
        self.connection_url = {
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "query": {
                "driver": f"ODBC Driver {self.odbcdriver_version} for SQL Server",
                "TrustServerCertificate": "yes",
            },
        }
        self.engine_args = {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "echo": self.echo,
        }

        if not self.username or not self.password:
            raise RuntimeError("Missing username or password")

    def __enter__(self):
        with self.engine() as self.temp_engine:
            session_maker = sessionmaker(bind=self.temp_engine,
                                         class_=Session,
                                         autoflush=True,
                                         expire_on_commit=True)
        with session_maker.begin() as self.session:
            self._test_connection_sync(self.session)
            return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
        if self.temp_engine:
            self.temp_engine.dispose()

    async def __aenter__(self):
        async with self.async_engine() as self.temp_engine:
            session_maker = sessionmaker(bind=self.temp_engine,
                                         class_=AsyncSession,
                                         autoflush=True,
                                         expire_on_commit=False)
        async with session_maker.begin() as self.session:
            await self._test_connection_async(self.session)
            return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self.temp_engine:
            await self.temp_engine.dispose()

    @contextmanager
    def engine(self) -> Engine:
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.sync_driver
        )
        _engine_args = {
            "url": _connection_url,
        }
        _engine = create_engine(**_engine_args)
        _engine.update_execution_options(schema_translate_map={None: self.schema})
        yield _engine

    @asynccontextmanager
    async def async_engine(self) -> AsyncGenerator:
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.async_driver
        )
        _engine_args = {
            "url": _connection_url,
        }
        _engine = create_async_engine(**_engine_args)
        _engine.update_execution_options(schema_translate_map={None: self.schema})
        yield _engine

    def _test_connection_sync(self, session: Session) -> None:
        host_url = URL.create(
            drivername=self.sync_driver,
            username=self.username,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"schema": self.schema},
        )
        test_connection = TestConnections(sync_session=session, host_url=host_url)
        test_connection.test_connection_sync()

    async def _test_connection_async(self, session: AsyncSession) -> None:
        host_url = URL.create(
            drivername=self.async_driver,
            username=self.username,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"schema": self.schema},
        )
        test_connection = TestConnections(async_session=session, host_url=host_url)
        await test_connection.test_connection_async()
