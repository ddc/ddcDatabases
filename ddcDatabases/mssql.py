# -*- coding: utf-8 -*-
import sys
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy.engine import base, URL
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session
from .db_utils import DBUtils, DBUtilsAsync
from .settings import MSSQLSettings


class MSSQL:
    def __init__(
        self,
        host: str = None,
        username: str = None,
        password: str = None,
        port: int = None,
        database: str = None,
        schema: str = None,
        echo: bool = None,
        pool_size: int = None,
        max_overflow: int = None,
        odbcdriver_version: int = None,
        async_driver: str = None,
        sync_driver: str = None,
    ):
        _settings = MSSQLSettings()
        self.engine = None
        self.session = None
        self.host = _settings.host if not host else host
        self.username = _settings.user if not username else username
        self.password = _settings.password if not password else password
        self.port = int(_settings.port if not port else int(port))
        self.database = _settings.database if not database else database
        self.schema = _settings.db_schema if not schema else schema
        self.echo = _settings.echo if not echo else echo
        self.pool_size = int(_settings.pool_size
                             if not pool_size else int(pool_size))
        self.max_overflow = int(_settings.overflow
                                if not max_overflow else int(max_overflow))
        self.odbcdriver_version = int(_settings.odbcdriver_version
                                      if not odbcdriver_version
                                      else int(odbcdriver_version))
        self.async_driver = (_settings.async_driver
                             if not async_driver else async_driver)
        self.sync_driver = (_settings.sync_driver
                            if not sync_driver else sync_driver)
        self.query = {
            "driver": f"ODBC Driver {self.odbcdriver_version} for SQL Server",
            "TrustServerCertificate": "yes",
        }

    def __enter__(self):
        self.engine = self._get_engine(sync=True)
        self.session = Session(self.engine)
        self.engine.dispose()
        self._test_connection_sync(self.session)
        db_utils = DBUtils(self.session)
        return db_utils

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    async def __aenter__(self):
        self.engine = self._get_engine(sync=False)
        self.session = AsyncSession(self.engine)
        await self.engine.dispose()
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
        dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        host_url = URL.create(
            drivername=self.sync_driver,
            username=self.username,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"schema": self.schema},
        )

        try:
            session.execute(sa.text("SELECT 1"))
            sys.stdout.write(f"[{dt}]:[INFO]:Connection to database successful | {host_url}\n")
        except Exception as e:
            self.session.close()
            sys.stderr.write(
                f"[{dt}]:[ERROR]:Connection to datatabse failed | "
                f"{host_url} | "
                f"{repr(e)}\n"
            )
            raise

    async def _test_connection_async(self, session: AsyncSession) -> None:
        dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        host_url = URL.create(
            drivername=self.async_driver,
            username=self.username,
            host=self.host,
            port=self.port,
            database=self.database,
            query={"schema": self.schema},
        )

        try:
            await session.execute(sa.text("SELECT 1"))
            sys.stdout.write(f"[{dt}]:[INFO]:Connection to database successful | {host_url}\n")
        except Exception as e:
            await self.session.close()
            sys.stderr.write(
                f"[{dt}]:[ERROR]:Connection to datatabse failed | "
                f"{host_url} | "
                f"{repr(e)}\n"
            )
            raise
