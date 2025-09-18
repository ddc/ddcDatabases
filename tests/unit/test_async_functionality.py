import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator
import pytest
import sqlalchemy as sa
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class AsyncTestModel(Base):
    __tablename__ = 'async_test_model'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    enabled = Column(Boolean, default=True)


class ConcreteAsyncTestConnection:
    """Concrete implementation of BaseConnection for async testing"""

    @staticmethod
    def create_test_connection(connection_url, engine_args, autoflush, expire_on_commit, sync_driver, async_driver):
        """Create a concrete test implementation of BaseConnection"""
        from ddcDatabases.db_utils import BaseConnection

        class TestableAsyncBaseConnection(BaseConnection):
            @contextmanager
            def _get_engine(self) -> Generator[Engine, None, None]:
                from sqlalchemy.engine import create_engine, URL
                _connection_url = URL.create(
                    drivername=self.sync_driver,
                    **self.connection_url,
                )
                _engine_args = {
                    "url": _connection_url,
                    **self.engine_args,
                }
                _engine = create_engine(**_engine_args)
                yield _engine
                _engine.dispose()

            @asynccontextmanager
            async def _get_async_engine(self) -> AsyncGenerator[AsyncEngine, None]:
                from sqlalchemy.ext.asyncio import create_async_engine
                from sqlalchemy.engine import URL
                _connection_url = URL.create(
                    drivername=self.async_driver,
                    **self.connection_url,
                )
                _engine_args = {
                    "url": _connection_url,
                    **self.engine_args,
                }
                _engine = create_async_engine(**_engine_args)
                yield _engine
                await _engine.dispose()

        return TestableAsyncBaseConnection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=autoflush,
            expire_on_commit=expire_on_commit,
            sync_driver=sync_driver,
            async_driver=async_driver,
        )


@pytest.mark.asyncio
class TestAsyncBaseConnection:
    """Test async functionality of BaseConnection"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases.db_utils import BaseConnection

        self.BaseConnection = BaseConnection

    async def test_async_context_manager_entry(self):
        """Test async context manager __aenter__"""
        connection_url = {"host": "localhost", "database": "test"}
        engine_args = {"echo": False}

        conn = ConcreteAsyncTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=True,
            expire_on_commit=False,
            sync_driver="postgresql+psycopg2",
            async_driver="postgresql+asyncpg",
        )

        with (
            patch.object(conn, '_get_async_engine') as mock_get_engine,
            patch('ddcDatabases.db_utils.async_sessionmaker') as mock_sessionmaker,
            patch.object(conn, '_test_connection_async') as mock_test_conn,
        ):

            mock_engine = AsyncMock()
            mock_get_engine.return_value.__aenter__.return_value = mock_engine

            mock_session = AsyncMock()
            mock_session_maker = MagicMock()
            mock_session_maker.begin.return_value.__aenter__.return_value = mock_session
            mock_sessionmaker.return_value = mock_session_maker

            async with conn as session:
                assert session is mock_session
                assert conn.is_connected == True

            mock_sessionmaker.assert_called_once()
            mock_test_conn.assert_called_once_with(mock_session)

    async def test_async_context_manager_exit(self):
        """Test async context manager __aexit__"""
        connection_url = {
            "host": "localhost",
            "database": "test",
        }
        engine_args = {"echo": False}

        conn = ConcreteAsyncTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=True,
            expire_on_commit=False,
            sync_driver="postgresql+psycopg2",
            async_driver="postgresql+asyncpg",
        )

        # Mock the session and engine for cleanup testing
        mock_session = AsyncMock()
        mock_engine = AsyncMock()

        conn.session = mock_session
        conn._temp_engine = mock_engine

        await conn.__aexit__(None, None, None)

        mock_session.close.assert_called_once()
        mock_engine.dispose.assert_called_once()
        assert conn.is_connected == False

    async def test_get_async_engine(self):
        """Test _get_async_engine method"""
        connection_url = {
            "host": "localhost",
            "database": "test",
        }
        engine_args = {"echo": False}

        conn = ConcreteAsyncTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=True,
            expire_on_commit=False,
            sync_driver="postgresql+psycopg2",
            async_driver="postgresql+asyncpg",
        )

        # Test the actual _get_async_engine method with real engine creation
        async with conn._get_async_engine() as engine:
            # Our concrete implementation creates real engines
            assert engine is not None
            assert hasattr(engine, 'dispose')  # Engine should have dispose method
            assert hasattr(engine, 'url')  # Should have URL attribute


@pytest.mark.asyncio
class TestDBUtilsAsync:
    """Test DBUtilsAsync functionality"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases import DBUtilsAsync

        self.DBUtilsAsync = DBUtilsAsync

    async def test_fetchall_success(self):
        """Test successful async fetchall operation"""
        mock_session = AsyncMock()
        mock_cursor = MagicMock()
        mock_mappings = MagicMock()
        mock_result = [{"id": 1, "name": "test"}]

        mock_session.execute.return_value = mock_cursor
        mock_cursor.mappings.return_value = mock_mappings
        mock_mappings.all.return_value = mock_result

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(AsyncTestModel)
        result = await db_utils.fetchall(stmt)

        assert result == mock_result
        mock_session.execute.assert_called_once_with(stmt)
        mock_cursor.mappings.assert_called_once()
        mock_mappings.all.assert_called_once()
        mock_cursor.close.assert_called_once()

    async def test_fetchall_exception_handling(self):
        """Test fetchall exception handling"""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Query failed")

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(AsyncTestModel)

        # The custom exception will re-raise the original exception
        with pytest.raises(Exception, match="Query failed"):
            await db_utils.fetchall(stmt)

        mock_session.rollback.assert_called_once()

    async def test_fetchvalue_success(self):
        """Test successful async fetchvalue operation"""
        mock_session = AsyncMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("test_value",)

        mock_session.execute.return_value = mock_cursor

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(AsyncTestModel.name)
        result = await db_utils.fetchvalue(stmt)

        assert result == "test_value"
        mock_session.execute.assert_called_once_with(stmt)
        mock_cursor.fetchone.assert_called_once()
        mock_cursor.close.assert_called_once()

    async def test_fetchvalue_none_result(self):
        """Test fetchvalue with None result"""
        mock_session = AsyncMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None

        mock_session.execute.return_value = mock_cursor

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(AsyncTestModel.name)
        result = await db_utils.fetchvalue(stmt)

        assert result is None

    async def test_fetchvalue_exception_handling(self):
        """Test fetchvalue exception handling"""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Query failed")

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(AsyncTestModel.name)

        with pytest.raises(Exception, match="Query failed"):
            await db_utils.fetchvalue(stmt)

        mock_session.rollback.assert_called_once()

    async def test_insert_success(self):
        """Test successful async insert operation"""
        mock_session = AsyncMock()
        # Make add a regular (non-coroutine) method
        mock_session.add = MagicMock()

        db_utils = self.DBUtilsAsync(mock_session)
        test_obj = AsyncTestModel(id=1, name="test")

        await db_utils.insert(test_obj)

        mock_session.add.assert_called_once_with(test_obj)
        mock_session.commit.assert_called_once()

    async def test_insert_exception_handling(self):
        """Test insert exception handling"""
        mock_session = AsyncMock()
        # Make add a regular (non-coroutine) method that raises
        mock_session.add = MagicMock(side_effect=Exception("Insert failed"))

        db_utils = self.DBUtilsAsync(mock_session)
        test_obj = AsyncTestModel(id=1, name="test")

        with pytest.raises(Exception, match="Insert failed"):
            await db_utils.insert(test_obj)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception

    async def test_insertbulk_success(self):
        """Test successful async bulk insert operation"""
        mock_session = AsyncMock()
        mock_session.run_sync = AsyncMock()

        db_utils = self.DBUtilsAsync(mock_session)
        bulk_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

        result = await db_utils.insertbulk(AsyncTestModel, bulk_data)

        # Verify the method returns None
        assert result is None
        
        # Verify session methods were called
        mock_session.run_sync.assert_called_once()
        mock_session.commit.assert_called_once()

    async def test_insertbulk_exception_handling(self):
        """Test bulk insert exception handling"""
        mock_session = AsyncMock()
        # Make run_sync raise an exception
        mock_session.run_sync.side_effect = Exception("Bulk insert failed")

        db_utils = self.DBUtilsAsync(mock_session)
        bulk_data = [{"id": 1, "name": "test1"}]

        with pytest.raises(Exception, match="Bulk insert failed"):
            await db_utils.insertbulk(AsyncTestModel, bulk_data)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception

    async def test_deleteall_success(self):
        """Test successful async delete all operation"""
        mock_session = AsyncMock()

        db_utils = self.DBUtilsAsync(mock_session)

        await db_utils.deleteall(AsyncTestModel)

        mock_session.execute.assert_called_once()
        # Verify that delete statement was created correctly
        call_args = mock_session.execute.call_args[0][0]
        assert hasattr(call_args, 'table')  # Should be a delete statement
        mock_session.commit.assert_called_once()

    async def test_deleteall_exception_handling(self):
        """Test delete all exception handling"""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Delete failed")

        db_utils = self.DBUtilsAsync(mock_session)

        with pytest.raises(Exception, match="Delete failed"):
            await db_utils.deleteall(AsyncTestModel)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception

    async def test_execute_success(self):
        """Test successful async execute operation"""
        mock_session = AsyncMock()

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.text("UPDATE async_test_model SET name = 'updated'")

        await db_utils.execute(stmt)

        mock_session.execute.assert_called_once_with(stmt)
        mock_session.commit.assert_called_once()

    async def test_execute_exception_handling(self):
        """Test execute exception handling"""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Execute failed")

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.text("UPDATE async_test_model SET name = 'updated'")

        with pytest.raises(Exception, match="Execute failed"):
            await db_utils.execute(stmt)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception


@pytest.mark.asyncio
class TestAsyncIntegration:
    """Test async integration scenarios"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases import DBUtilsAsync

        self.DBUtilsAsync = DBUtilsAsync

    async def test_async_workflow_simulation(self):
        """Test a complete async workflow simulation"""
        # Mock all components for a full async workflow test
        mock_session = AsyncMock()
        mock_cursor = MagicMock()
        mock_mappings = MagicMock()

        # Setup mock responses
        mock_session.execute.return_value = mock_cursor
        mock_cursor.mappings.return_value = mock_mappings
        mock_mappings.all.return_value = [{"id": 1, "name": "test", "enabled": True}]
        mock_cursor.fetchone.return_value = ("test",)

        # Make session methods that aren't coroutines regular methods
        mock_session.add = MagicMock()

        db_utils = self.DBUtilsAsync(mock_session)

        # Test fetchall
        stmt = sa.select(AsyncTestModel)
        results = await db_utils.fetchall(stmt)
        assert len(results) == 1
        assert results[0]["name"] == "test"

        # Test fetchvalue
        stmt = sa.select(AsyncTestModel.name)
        value = await db_utils.fetchvalue(stmt)
        assert value == "test"

        # Test insert
        test_obj = AsyncTestModel(id=2, name="new_test")
        await db_utils.insert(test_obj)
        mock_session.add.assert_called_with(test_obj)

        # Test execute
        stmt = sa.text("UPDATE async_test_model SET enabled = true")
        await db_utils.execute(stmt)

        # Verify operations were called
        assert mock_session.execute.call_count >= 3  # fetchall, fetchvalue, execute
        assert mock_session.commit.call_count >= 2  # insert and execute

    async def test_concurrent_async_operations(self):
        """Test concurrent async operations"""
        mock_session1 = AsyncMock()
        mock_session2 = AsyncMock()

        mock_cursor1 = MagicMock()
        mock_cursor2 = MagicMock()

        mock_session1.execute.return_value = mock_cursor1
        mock_session2.execute.return_value = mock_cursor2

        mock_cursor1.fetchone.return_value = ("result1",)
        mock_cursor2.fetchone.return_value = ("result2",)

        db_utils1 = self.DBUtilsAsync(mock_session1)
        db_utils2 = self.DBUtilsAsync(mock_session2)

        # Run concurrent operations
        stmt1 = sa.select(AsyncTestModel.name).where(AsyncTestModel.id == 1)
        stmt2 = sa.select(AsyncTestModel.name).where(AsyncTestModel.id == 2)

        results = await asyncio.gather(db_utils1.fetchvalue(stmt1), db_utils2.fetchvalue(stmt2))

        assert results[0] == "result1"
        assert results[1] == "result2"

        # Verify both sessions were used
        mock_session1.execute.assert_called_once()
        mock_session2.execute.assert_called_once()

    async def test_async_exception_propagation(self):
        """Test that async exceptions are properly propagated"""
        mock_session = AsyncMock()

        # Setup different exceptions for different methods
        mock_session.execute.side_effect = [Exception("Fetch error"), Exception("Execute error")]

        db_utils = self.DBUtilsAsync(mock_session)

        # Test fetchall exception
        with pytest.raises(Exception, match="Fetch error"):
            await db_utils.fetchall(sa.select(AsyncTestModel))

        # Test execute exception
        with pytest.raises(Exception, match="Execute error"):
            await db_utils.execute(sa.text("SELECT 1"))

        # Verify rollback was called for both
        assert mock_session.rollback.call_count == 2


class TestAsyncCompatibility:
    """Test async compatibility and edge cases"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases import DBUtilsAsync
        from ddcDatabases.db_utils import BaseConnection

        self.DBUtilsAsync = DBUtilsAsync
        self.BaseConnection = BaseConnection

    def test_async_method_signatures(self):
        """Test that async methods have correct signatures"""
        import inspect

        db_utils = self.DBUtilsAsync(None)

        # Check that all main methods are coroutines
        assert inspect.iscoroutinefunction(db_utils.fetchall)
        assert inspect.iscoroutinefunction(db_utils.fetchvalue)
        assert inspect.iscoroutinefunction(db_utils.insert)
        assert inspect.iscoroutinefunction(db_utils.insertbulk)
        assert inspect.iscoroutinefunction(db_utils.deleteall)
        assert inspect.iscoroutinefunction(db_utils.execute)

    def test_base_connection_async_methods(self):
        """Test BaseConnection has async methods"""
        import inspect

        conn = ConcreteAsyncTestConnection.create_test_connection({}, {}, True, False, "sync", "async")

        # Check async context manager methods
        assert inspect.iscoroutinefunction(conn.__aenter__)
        assert inspect.iscoroutinefunction(conn.__aexit__)
        assert inspect.iscoroutinefunction(conn._test_connection_async)

        # Check async generator method
        method = getattr(conn, '_get_async_engine')
        assert inspect.ismethod(method) or inspect.isfunction(method)
