# -*- coding: utf-8 -*-
from typing import Optional
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession
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
        if not _settings.user or not _settings.password:
            raise RuntimeError("Missing username or password")

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
            "host": host or _settings.host,
            "port":  int(port or _settings.port),
            "database": database or _settings.database,
            "username": user or _settings.user,
            "password": password or _settings.password,
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

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self.autoflush,
            expire_on_commit=self.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
        )

    def _test_connection_sync(self, session: Session) -> None:
        del self.connection_url["password"]
        del self.connection_url["query"]
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.sync_driver,
            query={"schema": self.schema},
        )
        test_connection = TestConnections(sync_session=session, host_url=_connection_url)
        test_connection.test_connection_sync()

    async def _test_connection_async(self, session: AsyncSession) -> None:
        del self.connection_url["password"]
        del self.connection_url["query"]
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.async_driver,
            query={"schema": self.schema},
        )
        test_connection = TestConnections(async_session=session, host_url=_connection_url)
        await test_connection.test_connection_async()
