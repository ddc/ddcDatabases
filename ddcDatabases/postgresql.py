# -*- encoding: utf-8 -*-
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator, Optional
from sqlalchemy.engine import create_engine, Engine, URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from .db_utils import TestConnections
from .settings import PostgreSQLSettings


class PostgreSQL:
    """
    Class to handle PostgreSQL connections
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        echo: Optional[bool] = None,
    ):
        _settings = PostgreSQLSettings()
        self.host = host or _settings.host
        self.username = username or _settings.username
        self.password = password or _settings.password
        self.port = port or int(_settings.port)
        self.database = database or _settings.database
        self.echo = echo or _settings.echo

        self.temp_engine: Optional[Engine | AsyncEngine] = None
        self.session: Optional[Session | AsyncSession] = None
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.connection_url = {
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "database": self.database,
        }
        self.engine_args = {
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
    def engine(self) -> Generator:
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.sync_driver
        )
        _engine_args = {
            "url": _connection_url,
        }
        _engine = create_engine(**_engine_args)
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
        yield _engine

    def _test_connection_sync(self, session: Session) -> None:
        host_url = URL.create(
            drivername=self.sync_driver,
            username=self.username,
            host=self.host,
            port=self.port,
            database=self.database,
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
        )
        test_connection = TestConnections(async_session=session, host_url=host_url)
        await test_connection.test_connection_async()
