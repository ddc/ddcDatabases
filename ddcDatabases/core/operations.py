from __future__ import annotations

from .exceptions import (
    DBDeleteAllDataException,
    DBExecuteException,
    DBFetchAllException,
    DBFetchValueException,
    DBInsertBulkException,
    DBInsertSingleException,
)
from .retry import RetryPolicy, retry_operation, retry_operation_async
import sqlalchemy as sa
from sqlalchemy import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from typing import Any, Callable, Sequence, TypeVar

# Type variable for generic model types
T = TypeVar('T')


class DBUtils:
    __slots__ = ('session', 'retry_config')

    def __init__(self, session: Session, retry_config: RetryPolicy | None = None) -> None:
        self.session = session
        self.retry_config = retry_config or RetryPolicy()

    def _execute_with_retry(self, operation: Callable[[], T], operation_name: str) -> T:
        """Execute an operation with retry logic if enabled."""
        if self.retry_config.enable_retry:
            return retry_operation(operation, self.retry_config, operation_name)
        return operation()

    def _fetchall_impl(self, stmt: Any, as_dict: bool = False) -> list[RowMapping] | list[dict]:
        try:
            cursor = self.session.execute(stmt)
            if as_dict:
                result = cursor.all()
                cursor.close()
                # noinspection PyProtectedMember
                return [row._asdict() for row in result]  # noqa: SLF001
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

    def __init__(self, session: AsyncSession, retry_config: RetryPolicy | None = None):
        self.session = session
        self.retry_config = retry_config or RetryPolicy()

    async def _execute_with_retry(self, operation: Callable[[], Any], operation_name: str) -> Any:
        """Execute an async operation with retry logic if enabled."""
        if self.retry_config.enable_retry:
            return await retry_operation_async(operation, self.retry_config, operation_name)
        return await operation()

    async def _fetchall_impl(self, stmt: Any, as_dict: bool = False) -> list[RowMapping] | list[dict]:
        try:
            cursor = await self.session.execute(stmt)
            if as_dict:
                result = cursor.all()
                cursor.close()
                # noinspection PyProtectedMember
                return [row._asdict() for row in result]  # noqa: SLF001
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
        return await self._execute_with_retry(lambda: self._insertbulk_impl(model, list_data, batch_size), "insertbulk")

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
