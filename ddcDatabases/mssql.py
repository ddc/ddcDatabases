# -*- coding: utf-8 -*-
import sqlalchemy as sa
from sqlalchemy.engine import base, URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine
)
from sqlalchemy.orm import Session, sessionmaker
from .db_utils import DBUtils, DBUtilsAsync, TestConnections
from .settings import MSSQLSettings


class MSSQL:
    """
    Class to handle MSSQL connections
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        username: str = None,
        password: str = None,
        database: str = None,
        schema: str = None,
        echo: bool = None,
        pool_size: int = None,
        max_overflow: int = None,

    ):
        _settings = MSSQLSettings()
        self.session = None
        self.host = _settings.host if not host else host
        self.username = _settings.username if not username else username
        self.password = _settings.password if not password else password
        self.port = int(_settings.port if not port else int(port))
        self.database = _settings.database if not database else database
        self.schema = _settings.db_schema if not schema else schema
        self.echo = bool(_settings.echo if not echo else bool(echo))
        self.pool_size = int(_settings.pool_size
                             if not pool_size else int(pool_size))
        self.max_overflow = int(_settings.max_overflow
                                if not max_overflow else int(max_overflow))

        self.odbcdriver_version = int(_settings.odbcdriver_version)
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.query = {
            "driver": f"ODBC Driver {self.odbcdriver_version} for SQL Server",
            "TrustServerCertificate": "yes",
        }

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
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            query=self.query,
        )

        _engine_args = {
            "url": _connection_url,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "echo": self.echo,
        }

        if sync:
            engine = sa.create_engine(**_engine_args)
        else:
            engine = create_async_engine(**_engine_args)

        engine.update_execution_options(schema_translate_map={None: self.schema})
        return engine

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
