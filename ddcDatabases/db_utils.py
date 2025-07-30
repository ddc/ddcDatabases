from __future__ import annotations
import logging
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import AsyncGenerator, Generator
import sqlalchemy as sa
from sqlalchemy import RowMapping
from sqlalchemy.engine import create_engine, Engine, URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from ddcDatabases.exceptions import (
    DBDeleteAllDataException,
    DBExecuteException,
    DBFetchAllException,
    DBFetchValueException,
    DBInsertBulkException,
    DBInsertSingleException,
)


class BaseConnection:
    def __init__(
        self,
        connection_url: dict,
        engine_args: dict,
        autoflush: bool,
        expire_on_commit: bool,
        sync_driver: str | None,
        async_driver: str | None,
    ):
        self.connection_url = connection_url
        self.engine_args = engine_args
        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.sync_driver = sync_driver
        self.async_driver = async_driver
        self.session: Session | AsyncSession | None = None
        self.is_connected = False
        self._temp_engine: Engine | AsyncEngine | None = None

    def __enter__(self):
        with self._get_engine() as self._temp_engine:
            session_maker = sessionmaker(
                bind=self._temp_engine,
                class_=Session,
                autoflush=self.autoflush or True,
                expire_on_commit=self.expire_on_commit or True,
            )
        with session_maker.begin() as self.session:
            self._test_connection_sync(self.session)
            self.is_connected = True
            return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
        if self._temp_engine:
            self._temp_engine.dispose()
        self.is_connected = False

    async def __aenter__(self):
        async with self._get_async_engine() as self._temp_engine:
            session_maker = async_sessionmaker(
                bind=self._temp_engine,
                class_=AsyncSession,
                autoflush=self.autoflush or True,
                expire_on_commit=self.expire_on_commit or False,
            )
        async with session_maker.begin() as self.session:
            await self._test_connection_async(self.session)
            self.is_connected = True
            return self.session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self._temp_engine:
            await self._temp_engine.dispose()
        self.is_connected = False

    @contextmanager
    def _get_engine(self) -> Generator[Engine, None, None]:
        _connection_url = URL.create(
            drivername=self.sync_driver,
            **self.connection_url,
        )
        _engine_args = {
            "url": _connection_url,
            "pool_size": 10,
            "max_overflow": 20,
            "pool_pre_ping": True,
            "pool_recycle": 3600,  # Recycle connections after 1 hour
            "query_cache_size": 1000,  # Enable query cache
            **self.engine_args,
        }
        _engine = create_engine(**_engine_args)
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
            "pool_size": 10,
            "max_overflow": 20,
            "pool_recycle": 3600,  # Recycle connections after 1 hour
            **self.engine_args,
        }
        _engine = create_async_engine(**_engine_args)
        yield _engine
        await _engine.dispose()

    def _test_connection_sync(self, session: Session) -> None:
        _connection_url_copy = self.connection_url.copy()
        _connection_url_copy.pop("password", None)
        _connection_url = URL.create(
            **_connection_url_copy,
            drivername=self.sync_driver,
        )
        test_connection = ConnectionTester(
            sync_session=session,
            host_url=_connection_url,
        )
        test_connection.test_connection_sync()

    async def _test_connection_async(self, session: AsyncSession) -> None:
        _connection_url_copy = self.connection_url.copy()
        _connection_url_copy.pop("password", None)
        _connection_url = URL.create(
            **_connection_url_copy,
            drivername=self.async_driver,
        )
        test_connection = ConnectionTester(
            async_session=session,
            host_url=_connection_url,
        )
        await test_connection.test_connection_async()


class ConnectionTester:
    def __init__(
        self,
        sync_session: Session | None = None,
        async_session: AsyncSession | None = None,
        host_url: URL | str = "",
    ):
        self.sync_session = sync_session
        self.async_session = async_session
        self.host_url = host_url
        self.dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        self.logger = logging.getLogger(__name__)
        self.failed_msg = "Connection to database failed"

    def test_connection_sync(self) -> bool:
        try:
            query_text = "SELECT 1 FROM dual" if "oracle" in str(self.sync_session.bind.url) else "SELECT 1"
            self.sync_session.execute(sa.text(query_text))
            return True
        except Exception as e:
            self.sync_session.close()
            error_msg = f"[{self.dt}]:[ERROR]:{self.failed_msg} | {self.host_url} | {e!r}"
            self.logger.error(error_msg)
            raise ConnectionRefusedError(f"{self.failed_msg} | {e!r}") from e

    async def test_connection_async(self) -> bool:
        try:
            query_text = "SELECT 1 FROM dual" if "oracle" in str(self.async_session.bind.url) else "SELECT 1"
            await self.async_session.execute(sa.text(query_text))
            return True
        except Exception as e:
            await self.async_session.close()
            error_msg = f"[{self.dt}]:[ERROR]:{self.failed_msg} | {self.host_url} | {e!r}"
            self.logger.error(error_msg)
            raise ConnectionRefusedError(f"{self.failed_msg} | {e!r}") from e


class DBUtils:
    def __init__(self, session: Session):
        self.session = session

    def fetchall(self, stmt) -> list[RowMapping]:
        try:
            cursor = self.session.execute(stmt)
            result = cursor.mappings().all()
            cursor.close()
            return list(result)
        except Exception as e:
            self.session.rollback()
            raise DBFetchAllException(e) from e

    def fetchvalue(self, stmt) -> str | None:
        try:
            cursor = self.session.execute(stmt)
            result = cursor.fetchone()
            cursor.close()
            return str(result[0]) if result else None
        except Exception as e:
            self.session.rollback()
            raise DBFetchValueException(e) from e

    def insert(self, stmt):
        try:
            self.session.add(stmt)
            self.session.commit()
            self.session.refresh(stmt)
            return stmt
        except Exception as e:
            self.session.rollback()
            raise DBInsertSingleException(e) from e

    def insertbulk(self, model, list_data: list[dict]) -> None:
        try:
            self.session.bulk_insert_mappings(model, list_data)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBInsertBulkException(e) from e

    def deleteall(self, model) -> None:
        try:
            self.session.query(model).delete()
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBDeleteAllDataException(e) from e

    def execute(self, stmt) -> None:
        try:
            self.session.execute(stmt)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBExecuteException(e) from e


class DBUtilsAsync:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def fetchall(self, stmt) -> list[RowMapping]:
        try:
            cursor = await self.session.execute(stmt)
            result = cursor.mappings().all()
            cursor.close()
            return list(result)
        except Exception as e:
            await self.session.rollback()
            raise DBFetchAllException(e) from e

    async def fetchvalue(self, stmt) -> str | None:
        try:
            cursor = await self.session.execute(stmt)
            result = cursor.fetchone()
            cursor.close()
            return str(result[0]) if result else None
        except Exception as e:
            await self.session.rollback()
            raise DBFetchValueException(e) from e

    async def insert(self, stmt):
        try:
            self.session.add(stmt)
            await self.session.commit()
            await self.session.refresh(stmt)
            return stmt
        except Exception as e:
            await self.session.rollback()
            raise DBInsertSingleException(e) from e

    async def insertbulk(self, model, list_data: list[dict]) -> None:
        try:
            await self.session.run_sync(lambda sync_session: sync_session.bulk_insert_mappings(model, list_data))
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise DBInsertBulkException(e) from e

    async def deleteall(self, model) -> None:
        try:
            await self.session.execute(sa.delete(model))
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise DBDeleteAllDataException(e) from e

    async def execute(self, stmt) -> None:
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise DBExecuteException(e) from e
