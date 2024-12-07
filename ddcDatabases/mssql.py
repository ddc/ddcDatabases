# -*- coding: utf-8 -*-
from typing import Optional
from sqlalchemy.engine import Engine, URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
)
from sqlalchemy.orm import Session
from .db_utils import BaseConn, TestConnections
from .settings import MSSQLSettings


class MSSQL(BaseConn):
    """
    Class to handle MSSQL connections
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        echo: Optional[bool] = None,
        pool_size: Optional[int] = None,
        max_overflow: Optional[int] = None,
        autoflush: Optional[bool] = None,
        expire_on_commit: Optional[bool] = None,
    ):
        _settings = MSSQLSettings()
        self.host = host or _settings.host
        self.user = user or _settings.user
        self.password = password or _settings.password
        self.port = port or int(_settings.port)
        self.database = database or _settings.database
        self.schema = schema or _settings.db_schema
        self.echo = echo or _settings.echo
        self.pool_size = pool_size or int(_settings.pool_size)
        self.max_overflow = max_overflow or int(_settings.max_overflow)

        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.odbcdriver_version = int(_settings.odbcdriver_version)
        self.connection_url = {
            "username": self.user,
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

        if not self.user or not self.password:
            raise RuntimeError("Missing username or password")

        super().__init__(
            host=self.host,
            port=self.port,
            user=self.user,
            database=self.database,
            autoflush=self.autoflush,
            expire_on_commit=self.expire_on_commit,
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
        )

    def _test_connection_sync(self, session: Session) -> None:
        host_url = URL.create(
            drivername=self.sync_driver,
            username=self.user,
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
            username=self.user,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"schema": self.schema},
        )
        test_connection = TestConnections(async_session=session, host_url=host_url)
        await test_connection.test_connection_async()
