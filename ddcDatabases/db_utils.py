from __future__ import annotations
import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Sequence, TypeVar
import sqlalchemy as sa
from sqlalchemy import RowMapping
from sqlalchemy.engine import Engine, URL
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncEngine, AsyncSession
from sqlalchemy.orm import Session, sessionmaker
from ddcDatabases.exceptions import (
    DBDeleteAllDataException,
    DBExecuteException,
    DBFetchAllException,
    DBFetchValueException,
    DBInsertBulkException,
    DBInsertSingleException,
)

# Type variable for generic model types
T = TypeVar('T')

# Logger for retry operations
logger = logging.getLogger(__name__)

# Keywords that indicate connection-related errors
CONNECTION_ERROR_KEYWORDS: frozenset[str] = frozenset(
    {
        "connection",
        "connect",
        "timeout",
        "timed out",
        "refused",
        "reset",
        "broken pipe",
        "network",
        "socket",
        "server closed",
        "lost connection",
        "server has gone away",
        "communication link",
        "operational error",
        "connection refused",
        "connection reset",
        "connection timed out",
        "no route to host",
        "host unreachable",
        "name or service not known",
        "temporary failure",
        "eof detected",
        "ssl error",
        "handshake failure",
        "authentication failed",
        "too many connections",
        "connection pool",
        "pool exhausted",
    }
)


@dataclass(slots=True, frozen=True)
class RetryConfig:
    """Configuration for retry behavior on database operations."""

    enable_retry: bool = True
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 30.0
    jitter: float = 0.1

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be non-negative")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")


def _is_connection_error(exc: Exception) -> bool:
    """
    Detect if an exception is connection-related.

    Args:
        exc: The exception to check

    Returns:
        True if the exception appears to be connection-related
    """
    error_str = str(exc).lower()
    exc_type_name = type(exc).__name__.lower()

    # Check exception type name
    if any(keyword in exc_type_name for keyword in ("connection", "network", "timeout", "operational")):
        return True

    # Check error message for keywords
    return any(keyword in error_str for keyword in CONNECTION_ERROR_KEYWORDS)


def _calculate_retry_delay(attempt: int, config: RetryConfig) -> float:
    """
    Calculate the delay before the next retry using exponential backoff with jitter.

    Args:
        attempt: Current retry attempt (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    # Exponential backoff: delay = initial_delay * 2^attempt
    base_delay = config.initial_delay * (2**attempt)

    # Cap at max_delay
    capped_delay = min(base_delay, config.max_delay)

    # Add jitter (randomize +/- jitter%)
    jitter_range = capped_delay * config.jitter
    jitter_offset = random.uniform(-jitter_range, jitter_range)

    return max(0, capped_delay + jitter_offset)


def _handle_retry_exception(
    e: Exception,
    attempt: int,
    config: RetryConfig,
    operation_name: str,
) -> float:
    """
    Handle an exception during retry operation.

    Args:
        e: The exception that was raised
        attempt: Current retry attempt (0-indexed)
        config: Retry configuration
        operation_name: Name of the operation for logging

    Returns:
        Delay in seconds before next retry

    Raises:
        The exception if it's not retryable or max retries reached
    """
    if not _is_connection_error(e):
        # Not a connection error, don't retry
        raise

    if attempt >= config.max_retries:
        # No more retries
        logger.error(f"[{operation_name}] All {config.max_retries + 1} attempts failed. Last error: {e!r}")
        raise

    delay = _calculate_retry_delay(attempt, config)
    logger.warning(
        f"[{operation_name}] Attempt {attempt + 1}/{config.max_retries + 1} failed: {e!r}. "
        f"Retrying in {delay:.2f}s..."
    )
    return delay


def _retry_operation(
    operation: Callable[[], T],
    config: RetryConfig,
    operation_name: str = "operation",
) -> T:
    """
    Execute an operation with retry logic (synchronous).

    Args:
        operation: Callable to execute
        config: Retry configuration
        operation_name: Name of the operation for logging

    Returns:
        Result of the operation

    Raises:
        The last exception if all retries fail
    """
    if not config.enable_retry:
        return operation()

    last_exception: Exception | None = None

    for attempt in range(config.max_retries + 1):
        try:
            return operation()
        except Exception as e:
            last_exception = e
            delay = _handle_retry_exception(e, attempt, config, operation_name)
            time.sleep(delay)

    # Should not reach here, but satisfy type checker
    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected state in retry logic")


async def _retry_operation_async(
    operation: Callable[[], Any],
    config: RetryConfig,
    operation_name: str = "operation",
) -> Any:
    """
    Execute an operation with retry logic (asynchronous).

    Args:
        operation: Async callable to execute
        config: Retry configuration
        operation_name: Name of the operation for logging

    Returns:
        Result of the operation

    Raises:
        The last exception if all retries fail
    """
    if not config.enable_retry:
        return await operation()

    last_exception: Exception | None = None

    for attempt in range(config.max_retries + 1):
        try:
            return await operation()
        except Exception as e:
            last_exception = e
            delay = _handle_retry_exception(e, attempt, config, operation_name)
            await asyncio.sleep(delay)

    # Should not reach here, but satisfy type checker
    if last_exception:
        raise last_exception
    raise RuntimeError("Unexpected state in retry logic")


class BaseConnection(ABC):
    __slots__ = (
        'connection_url',
        'engine_args',
        'autoflush',
        'expire_on_commit',
        'sync_driver',
        'async_driver',
        'session',
        'is_connected',
        '_temp_engine',
        'retry_config',
    )

    def __init__(
        self,
        connection_url: dict,
        engine_args: dict,
        autoflush: bool,
        expire_on_commit: bool,
        sync_driver: str | None,
        async_driver: str | None,
        retry_config: RetryConfig | None = None,
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
        self.retry_config = retry_config or RetryConfig()

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

        return _retry_operation(connect, self.retry_config, "sync_connect")

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:
        if self.session:
            self.session.close()
        if self._temp_engine:
            self._temp_engine.dispose()
        self.is_connected = False

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

        return await _retry_operation_async(connect, self.retry_config, "async_connect")

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        if self.session:
            await self.session.close()
        if self._temp_engine:
            await self._temp_engine.dispose()
        self.is_connected = False

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
    __slots__ = ('sync_session', 'async_session', 'host_url', 'dt', 'logger', 'failed_msg')

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
    __slots__ = ('session', 'retry_config')

    def __init__(self, session: Session, retry_config: RetryConfig | None = None) -> None:
        self.session = session
        self.retry_config = retry_config or RetryConfig()

    def _execute_with_retry(self, operation: Callable[[], T], operation_name: str) -> T:
        """Execute an operation with retry logic if enabled."""
        if self.retry_config.enable_retry:
            return _retry_operation(operation, self.retry_config, operation_name)
        return operation()

    def _fetchall_impl(self, stmt: Any, as_dict: bool = False) -> list[RowMapping] | list[dict]:
        try:
            cursor = self.session.execute(stmt)
            if as_dict:
                result = cursor.all()
                cursor.close()
                return [row._asdict() for row in result]
            else:
                result = cursor.mappings().all()
                cursor.close()
                return list(result)
        except Exception as e:
            self.session.rollback()
            raise DBFetchAllException(e) from e

    def fetchall(self, stmt: Any, as_dict: bool = False) -> list[RowMapping] | list[dict]:
        """
        Execute a SELECT statement and fetch all results.

        Args:
            stmt: SQLAlchemy statement or raw SQL string to execute
            as_dict: If True, returns list of dicts; if False, returns list of RowMapping objects

        Returns:
            List of query results as either RowMapping objects or dictionaries

        Raises:
            DBFetchAllException: If query execution fails
        """
        return self._execute_with_retry(lambda: self._fetchall_impl(stmt, as_dict), "fetchall")

    def _fetchvalue_impl(self, stmt: Any) -> str | None:
        try:
            cursor = self.session.execute(stmt)
            result = cursor.fetchone()
            cursor.close()
            return str(result[0]) if result else None
        except Exception as e:
            self.session.rollback()
            raise DBFetchValueException(e) from e

    def fetchvalue(self, stmt: Any) -> str | None:
        """
        Execute a SELECT statement and fetch a single scalar value.

        Args:
            stmt: SQLAlchemy statement or raw SQL string to execute

        Returns:
            String representation of the first column of the first row, or None if no results

        Raises:
            DBFetchValueException: If query execution fails
        """
        return self._execute_with_retry(lambda: self._fetchvalue_impl(stmt), "fetchvalue")

    def _insert_impl(self, stmt: Any) -> Any:
        try:
            self.session.add(stmt)
            self.session.commit()
            self.session.refresh(stmt)
            return stmt
        except Exception as e:
            self.session.rollback()
            raise DBInsertSingleException(e) from e

    def insert(self, stmt: Any) -> Any:
        """
        Insert a single record and return the inserted instance with updated fields.

        Args:
            stmt: SQLAlchemy model instance to insert

        Returns:
            The inserted model instance with refreshed data (including auto-generated IDs)

        Raises:
            DBInsertSingleException: If insert operation fails
        """
        return self._execute_with_retry(lambda: self._insert_impl(stmt), "insert")

    def _insertbulk_impl(self, model: type[T], list_data: Sequence[dict[str, Any]], batch_size: int = 1000) -> None:
        try:
            if not list_data:
                return

            for i in range(0, len(list_data), batch_size):
                batch = list_data[i : i + batch_size]
                self.session.bulk_insert_mappings(model, batch, return_defaults=False)

            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBInsertBulkException(e) from e

    def insertbulk(self, model: type[T], list_data: Sequence[dict[str, Any]], batch_size: int = 1000) -> None:
        """
        Bulk insert data using the most efficient method available.

        This method prioritizes performance over returning inserted records.
        Use the regular insert() method if you need the inserted instances back.

        Args:
            model: The SQLAlchemy model class
            list_data: List of dictionaries containing the data to insert
            batch_size: Number of records to insert per batch (default: 1000)

        Raises:
            DBInsertBulkException: If bulk insert operation fails
        """
        return self._execute_with_retry(lambda: self._insertbulk_impl(model, list_data, batch_size), "insertbulk")

    def _deleteall_impl(self, model: type[T]) -> None:
        try:
            self.session.query(model).delete()
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBDeleteAllDataException(e) from e

    def deleteall(self, model: type[T]) -> None:
        """
        Delete all records from a table.

        WARNING: This operation removes ALL data from the specified table.

        Args:
            model: The SQLAlchemy model class representing the table to clear

        Raises:
            DBDeleteAllDataException: If delete operation fails
        """
        return self._execute_with_retry(lambda: self._deleteall_impl(model), "deleteall")

    def _execute_impl(self, stmt: Any) -> None:
        try:
            self.session.execute(stmt)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBExecuteException(e) from e

    def execute(self, stmt: Any) -> None:
        """
        Execute a statement that doesn't return results (INSERT, UPDATE, DELETE).

        Args:
            stmt: SQLAlchemy statement or raw SQL string to execute

        Raises:
            DBExecuteException: If statement execution fails
        """
        return self._execute_with_retry(lambda: self._execute_impl(stmt), "execute")


class DBUtilsAsync:
    __slots__ = ('session', 'retry_config')

    def __init__(self, session: AsyncSession, retry_config: RetryConfig | None = None):
        self.session = session
        self.retry_config = retry_config or RetryConfig()

    async def _execute_with_retry(self, operation: Callable[[], Any], operation_name: str) -> Any:
        """Execute an async operation with retry logic if enabled."""
        if self.retry_config.enable_retry:
            return await _retry_operation_async(operation, self.retry_config, operation_name)
        return await operation()

    async def _fetchall_impl(self, stmt: Any, as_dict: bool = False) -> list[RowMapping] | list[dict]:
        try:
            cursor = await self.session.execute(stmt)
            if as_dict:
                result = cursor.all()
                cursor.close()
                return [row._asdict() for row in result]
            else:
                result = cursor.mappings().all()
                cursor.close()
                return list(result)
        except Exception as e:
            await self.session.rollback()
            raise DBFetchAllException(e) from e

    async def fetchall(self, stmt: Any, as_dict: bool = False) -> list[RowMapping] | list[dict]:
        """
        Execute a SELECT statement asynchronously and fetch all results.

        Args:
            stmt: SQLAlchemy statement or raw SQL string to execute
            as_dict: If True, returns list of dicts; if False, returns list of RowMapping objects

        Returns:
            List of query results as either RowMapping objects or dictionaries

        Raises:
            DBFetchAllException: If query execution fails
        """
        return await self._execute_with_retry(lambda: self._fetchall_impl(stmt, as_dict), "fetchall")

    async def _fetchvalue_impl(self, stmt: Any) -> str | None:
        try:
            cursor = await self.session.execute(stmt)
            result = cursor.fetchone()
            cursor.close()
            return str(result[0]) if result else None
        except Exception as e:
            await self.session.rollback()
            raise DBFetchValueException(e) from e

    async def fetchvalue(self, stmt: Any) -> str | None:
        """
        Execute a SELECT statement asynchronously and fetch a single scalar value.

        Args:
            stmt: SQLAlchemy statement or raw SQL string to execute

        Returns:
            String representation of the first column of the first row, or None if no results

        Raises:
            DBFetchValueException: If query execution fails
        """
        return await self._execute_with_retry(lambda: self._fetchvalue_impl(stmt), "fetchvalue")

    async def _insert_impl(self, stmt: Any) -> Any:
        try:
            self.session.add(stmt)
            await self.session.commit()
            await self.session.refresh(stmt)
            return stmt
        except Exception as e:
            await self.session.rollback()
            raise DBInsertSingleException(e) from e

    async def insert(self, stmt: Any) -> Any:
        """
        Insert a single record asynchronously and return the inserted instance with updated fields.

        Args:
            stmt: SQLAlchemy model instance to insert

        Returns:
            The inserted model instance with refreshed data (including auto-generated IDs)

        Raises:
            DBInsertSingleException: If insert operation fails
        """
        return await self._execute_with_retry(lambda: self._insert_impl(stmt), "insert")

    async def _insertbulk_impl(
        self, model: type[T], list_data: Sequence[dict[str, Any]], batch_size: int = 1000
    ) -> None:
        try:
            if not list_data:
                return

            for i in range(0, len(list_data), batch_size):
                batch = list_data[i : i + batch_size]

                def _bulk_insert(session: Session, data: Sequence[dict[str, Any]] = batch) -> None:
                    session.bulk_insert_mappings(model, data, return_defaults=False)

                await self.session.run_sync(_bulk_insert)

            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise DBInsertBulkException(e) from e

    async def insertbulk(self, model: type[T], list_data: Sequence[dict[str, Any]], batch_size: int = 1000) -> None:
        """
        Bulk insert data using the most efficient method available.

        This method prioritizes performance over returning inserted records.
        Use the regular insert() method if you need the inserted instances back.

        Args:
            model: The SQLAlchemy model class
            list_data: List of dictionaries containing the data to insert
            batch_size: Number of records to insert per batch (default: 1000)

        Raises:
            DBInsertBulkException: If bulk insert operation fails
        """
        return await self._execute_with_retry(
            lambda: self._insertbulk_impl(model, list_data, batch_size), "insertbulk"
        )

    async def _deleteall_impl(self, model: type[T]) -> None:
        try:
            stmt = sa.delete(model)
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise DBDeleteAllDataException(e) from e

    async def deleteall(self, model: type[T]) -> None:
        """
        Delete all records from a table asynchronously.

        WARNING: This operation removes ALL data from the specified table.

        Args:
            model: The SQLAlchemy model class representing the table to clear

        Raises:
            DBDeleteAllDataException: If delete operation fails
        """
        return await self._execute_with_retry(lambda: self._deleteall_impl(model), "deleteall")

    async def _execute_impl(self, stmt: Any) -> None:
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise DBExecuteException(e) from e

    async def execute(self, stmt: Any) -> None:
        """
        Execute a statement asynchronously that doesn't return results (INSERT, UPDATE, DELETE).

        Args:
            stmt: SQLAlchemy statement or raw SQL string to execute

        Raises:
            DBExecuteException: If statement execution fails
        """
        return await self._execute_with_retry(lambda: self._execute_impl(stmt), "execute")
