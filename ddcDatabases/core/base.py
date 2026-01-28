from __future__ import annotations

import logging
import sqlalchemy as sa
from .configs import BaseOperationRetryConfig, BaseRetryConfig
from .retry import retry_operation, retry_operation_async
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from datetime import datetime
from sqlalchemy.engine import URL, Engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, sessionmaker
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


class BaseConnection(ABC):
    __slots__ = (
        "connection_url",
        "engine_args",
        "autoflush",
        "expire_on_commit",
        "sync_driver",
        "async_driver",
        "session",
        "is_connected",
        "_temp_engine",
        "conn_retry_config",
        "op_retry_config",
        "logger",
    )

    def __init__(
        self,
        connection_url: dict,
        engine_args: dict,
        autoflush: bool,
        expire_on_commit: bool,
        sync_driver: str | None,
        async_driver: str | None,
        conn_retry_config: BaseRetryConfig | None = None,
        op_retry_config: BaseOperationRetryConfig | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.connection_url = connection_url
        self.engine_args = engine_args
        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.sync_driver = sync_driver
        self.async_driver = async_driver
        self.session: Session | AsyncSession | None = None
        self.is_connected = False
        self._temp_engine: Engine | AsyncEngine | None = None
        self.conn_retry_config = conn_retry_config or BaseRetryConfig()
        self.op_retry_config = op_retry_config or BaseOperationRetryConfig()
        self.logger = logger if logger is not None else _logger

    def __enter__(self) -> Session:
        def connect() -> Session:
            with self._get_engine() as self._temp_engine:
                session_maker = sessionmaker(
                    bind=self._temp_engine,
                    class_=Session,
                    autoflush=self.autoflush,
                    expire_on_commit=self.expire_on_commit,
                )
            with session_maker.begin() as self.session:
                self._test_connection_sync(self.session)
                self.is_connected = True
                return self.session

        return retry_operation(connect, self.conn_retry_config, "sync_connect", logger=self.logger)

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        if self.session:
            self.session.close()
        if self._temp_engine:
            self._temp_engine.dispose()
        self.is_connected = False
        self.logger.debug("Disconnected")

    async def __aenter__(self) -> AsyncSession:
        async def connect() -> AsyncSession:
            async with self._get_async_engine() as self._temp_engine:
                session_maker = async_sessionmaker(
                    bind=self._temp_engine,
                    class_=AsyncSession,
                    autoflush=self.autoflush,
                    expire_on_commit=self.expire_on_commit,
                )
            async with session_maker.begin() as self.session:
                await self._test_connection_async(self.session)
                self.is_connected = True
                return self.session

        return await retry_operation_async(connect, self.conn_retry_config, "async_connect", logger=self.logger)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self.session:
            await self.session.close()
        if self._temp_engine:
            await self._temp_engine.dispose()
        self.is_connected = False
        self.logger.debug("Disconnected")

    @abstractmethod
    def _get_engine(self) -> AbstractContextManager[Engine]:
        pass

    @abstractmethod
    def _get_async_engine(self) -> AbstractAsyncContextManager[AsyncEngine]:
        pass

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
            logger=self.logger,
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
            logger=self.logger,
        )
        await test_connection.test_connection_async()


class ConnectionTester:
    __slots__ = ("sync_session", "async_session", "host_url", "dt", "logger", "failed_msg")

    def __init__(
        self,
        sync_session: Session | None = None,
        async_session: AsyncSession | None = None,
        host_url: URL | str = "",
        logger: logging.Logger | None = None,
    ) -> None:
        self.sync_session = sync_session
        self.async_session = async_session
        self.host_url = host_url
        self.dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        self.logger = logger if logger is not None else _logger
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
