# -*- encoding: utf-8 -*-
import sqlalchemy as sa
from sqlalchemy.engine import base, URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from .db_utils import DBUtils, DBUtilsAsync, TestConnections
from .settings import PostgreSQLSettings


class PostgreSQL:
    """
    Class to handle PostgreSQL connections
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        username: str = None,
        password: str = None,
        database: str = None,
        echo: bool = None,
    ):
        _settings = PostgreSQLSettings()
        self.session = None
        self.host = _settings.host if not host else host
        self.username = _settings.username if not username else username
        self.password = _settings.password if not password else password
        self.port = int(_settings.port if not port else int(port))
        self.database = _settings.database if not database else database
        self.echo = _settings.echo if not echo else echo
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver

    def __enter__(self):
        engine = self._get_engine(sync=True)
        session_maker = sessionmaker(bind=engine,
                                     class_=Session,
                                     autoflush=True,
                                     expire_on_commit=True)
        engine.dispose()
        with session_maker.begin() as session:
            self.session = session
            self._test_connection_sync(self.session)
            db_utils = DBUtils(self.session)
            return db_utils

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    async def __aenter__(self):
        engine = self._get_engine(sync=False)
        session_maker = sessionmaker(bind=engine,
                                     class_=AsyncSession,
                                     autoflush=True,
                                     expire_on_commit=False)
        await engine.dispose()
        async with session_maker.begin() as session:
            self.session = session
            await self._test_connection_async(self.session)
            db_utils = DBUtilsAsync(self.session)
            return db_utils

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    def _get_engine(self, sync: bool = True) -> base.Engine | AsyncEngine:
        _connection_url = URL.create(
            drivername=self.sync_driver if sync else self.async_driver,
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )

        _engine_args = {
            "url": _connection_url,
            "echo": self.echo,
        }

        if sync:
            engine = sa.create_engine(**_engine_args)
        else:
            engine = create_async_engine(**_engine_args)

        return engine

    def _test_connection_sync(self, session: Session) -> None:
        host_url = URL.create(
            drivername=self.sync_driver,
            host=self.host,
            port=self.port,
            username=self.username,
            database=self.database,
        )
        test_connection = TestConnections(sync_session=session, host_url=host_url)
        test_connection.test_connection_sync()

    async def _test_connection_async(self, session: AsyncSession) -> None:
        host_url = URL.create(
            drivername=self.async_driver,
            host=self.host,
            port=self.port,
            username=self.username,
            database=self.database,
        )
        test_connection = TestConnections(async_session=session, host_url=host_url)
        await test_connection.test_connection_async()
