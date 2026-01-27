from contextlib import asynccontextmanager, contextmanager
from importlib.util import find_spec
import pytest
import sqlalchemy as sa
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import declarative_base
from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock, patch

POSTGRESQL_AVAILABLE = find_spec("asyncpg") is not None and find_spec("psycopg") is not None


Base = declarative_base()


class DatabaseModel(Base):
    __tablename__ = 'test_model'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    enabled = Column(Boolean, default=True)


class ConcreteTestConnection:
    """Concrete implementation of BaseConnection for testing"""

    @staticmethod
    def create_test_connection(
        connection_url,
        engine_args,
        autoflush,
        expire_on_commit,
        sync_driver,
        async_driver,
        conn_retry_config=None,
        op_retry_config=None,
    ):
        """Create a concrete test implementation of BaseConnection"""
        from ddcDatabases.core.base import BaseConnection

        class TestableBaseConnection(BaseConnection):
            @contextmanager
            def _get_engine(self) -> Generator[Engine, None, None]:
                from sqlalchemy.engine import URL, create_engine

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
                from sqlalchemy.engine import URL
                from sqlalchemy.ext.asyncio import create_async_engine

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

        return TestableBaseConnection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=autoflush,
            expire_on_commit=expire_on_commit,
            sync_driver=sync_driver,
            async_driver=async_driver,
            conn_retry_config=conn_retry_config,
            op_retry_config=op_retry_config,
        )


class TestBaseConnection:
    """Test BaseConnection class"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases.core.base import BaseConnection, ConnectionTester

        self.BaseConnection = BaseConnection
        self.ConnectionTester = ConnectionTester

    def test_init(self):
        """Test BaseConnection initialization"""
        connection_url = {
            "host": "localhost",
            "database": "test",
        }
        engine_args = {"echo": True}

        conn = ConcreteTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=True,
            expire_on_commit=False,
            sync_driver="postgresql+psycopg",
            async_driver="postgresql+asyncpg",
        )

        assert conn.connection_url == connection_url
        assert conn.engine_args == engine_args
        assert conn.autoflush == True
        assert conn.expire_on_commit == False
        assert conn.sync_driver == "postgresql+psycopg"
        assert conn.async_driver == "postgresql+asyncpg"
        assert conn.is_connected == False
        assert conn.session is None

    @pytest.mark.skipif(not POSTGRESQL_AVAILABLE, reason="PostgreSQL drivers not available")
    @patch('sqlalchemy.engine.create_engine')
    def test_get_engine(self, mock_create_engine):
        """Test _get_engine context manager"""
        connection_url = {
            "host": "localhost",
            "database": "test",
        }
        engine_args = {"echo": True}

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        conn = ConcreteTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=True,
            expire_on_commit=False,
            sync_driver="postgresql+psycopg",
            async_driver="postgresql+asyncpg",
        )

        with conn._get_engine() as engine:
            assert engine is mock_engine
            mock_create_engine.assert_called_once()

        mock_engine.dispose.assert_called_once()

    def test_test_connection_sync_non_oracle(self):
        """Test connection test for non-Oracle database"""
        mock_session = MagicMock()
        mock_session.bind.url = "postgresql://user:password@host/db"

        test_conn = self.ConnectionTester(sync_session=mock_session)
        result = test_conn.test_connection_sync()

        assert result == True
        mock_session.execute.assert_called_once()
        # Check that standard query was used
        call_args = mock_session.execute.call_args[0][0]
        assert "SELECT 1" in str(call_args)

    def test_test_connection_sync_exception(self):
        """Test connection test with exception"""
        mock_session = MagicMock()
        mock_session.bind.url = "postgresql://user:password@host/db"
        mock_session.execute.side_effect = Exception("Connection failed")

        test_conn = self.ConnectionTester(sync_session=mock_session)

        with pytest.raises(ConnectionRefusedError):
            test_conn.test_connection_sync()

        mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_connection_async_exception(self):
        """Test async connection test with exception"""
        mock_session = AsyncMock()
        mock_session.bind.url = "postgresql://user:password@host/db"
        mock_session.execute.side_effect = Exception("Connection failed")

        test_conn = self.ConnectionTester(async_session=mock_session)

        with pytest.raises(ConnectionRefusedError):
            await test_conn.test_connection_async()

        mock_session.close.assert_called_once()


class TestDBUtils:
    """Test DBUtils class"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases import DBUtils

        self.DBUtils = DBUtils

    def test_init(self):
        """Test DBUtils initialization"""
        mock_session = MagicMock()
        db_utils = self.DBUtils(mock_session)
        assert db_utils.session is mock_session

    def test_fetchall_success(self):
        """Test successful fetchall operation"""
        mock_session = MagicMock()
        mock_cursor = MagicMock()
        mock_mappings = MagicMock()
        mock_result = [{"id": 1, "name": "test"}]

        mock_session.execute.return_value = mock_cursor
        mock_cursor.mappings.return_value = mock_mappings
        mock_mappings.all.return_value = mock_result

        db_utils = self.DBUtils(mock_session)
        stmt = sa.select(DatabaseModel)
        result = db_utils.fetchall(stmt)

        assert result == mock_result
        mock_session.execute.assert_called_once_with(stmt)
        mock_cursor.mappings.assert_called_once()
        mock_mappings.all.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_fetchall_as_dict(self):
        """Test fetchall with as_dict=True"""
        mock_session = MagicMock()
        mock_cursor = MagicMock()

        # Create mock Row objects that have _asdict method
        mock_row1 = MagicMock()
        mock_row1._asdict.return_value = {"id": 1, "name": "test1"}
        mock_row2 = MagicMock()
        mock_row2._asdict.return_value = {"id": 2, "name": "test2"}

        mock_session.execute.return_value = mock_cursor
        mock_cursor.all.return_value = [mock_row1, mock_row2]

        db_utils = self.DBUtils(mock_session)
        stmt = sa.select(DatabaseModel)
        result = db_utils.fetchall(stmt, as_dict=True)

        assert result == [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        mock_session.execute.assert_called_once_with(stmt)
        mock_cursor.all.assert_called_once()
        mock_cursor.close.assert_called_once()

    def test_fetchall_exception(self):
        """Test fetchall with exception"""
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Query failed")

        db_utils = self.DBUtils(mock_session)
        stmt = sa.select(DatabaseModel)

        with pytest.raises(Exception, match="Query failed"):
            db_utils.fetchall(stmt)

        mock_session.rollback.assert_called_once()

    def test_fetchvalue_success(self):
        """Test successful fetchvalue operation"""
        mock_session = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("test_value",)

        mock_session.execute.return_value = mock_cursor

        db_utils = self.DBUtils(mock_session)
        stmt = sa.select(DatabaseModel.name)
        result = db_utils.fetchvalue(stmt)

        assert result == "test_value"
        mock_cursor.close.assert_called_once()

    def test_fetchvalue_none(self):
        """Test fetchvalue with None result"""
        mock_session = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None

        mock_session.execute.return_value = mock_cursor

        db_utils = self.DBUtils(mock_session)
        stmt = sa.select(DatabaseModel.name)
        result = db_utils.fetchvalue(stmt)

        assert result is None

    def test_fetchvalue_exception(self):
        """Test fetchvalue with exception"""
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Query failed")

        db_utils = self.DBUtils(mock_session)
        stmt = sa.select(DatabaseModel.name)

        with pytest.raises(Exception, match="Query failed"):
            db_utils.fetchvalue(stmt)

        mock_session.rollback.assert_called_once()

    def test_insert_success(self):
        """Test successful insert operation"""
        mock_session = MagicMock()

        db_utils = self.DBUtils(mock_session)
        test_obj = DatabaseModel(id=1, name="test")

        result = db_utils.insert(test_obj)

        assert result is test_obj
        mock_session.add.assert_called_once_with(test_obj)
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once_with(test_obj)

    def test_insert_exception(self):
        """Test insert with exception"""
        mock_session = MagicMock()
        mock_session.add.side_effect = Exception("Insert failed")

        db_utils = self.DBUtils(mock_session)
        test_obj = DatabaseModel(id=1, name="test")

        with pytest.raises(Exception, match="Insert failed"):
            db_utils.insert(test_obj)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception
        mock_session.refresh.assert_not_called()  # Should not refresh on exception

    def test_insertbulk_success(self):
        """Test successful bulk insert operation"""
        mock_session = MagicMock()

        db_utils = self.DBUtils(mock_session)
        bulk_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

        result = db_utils.insertbulk(DatabaseModel, bulk_data)

        # Verify the method returns None
        assert result is None

        # Verify session methods were called
        mock_session.bulk_insert_mappings.assert_called_once_with(DatabaseModel, bulk_data, return_defaults=False)
        mock_session.commit.assert_called_once()

    def test_insertbulk_empty_list(self):
        """Test bulk insert with empty list"""
        mock_session = MagicMock()

        db_utils = self.DBUtils(mock_session)
        bulk_data = []

        result = db_utils.insertbulk(DatabaseModel, bulk_data)

        # Verify the method returns None for empty list
        assert result is None

        # Verify no database operations were performed
        mock_session.bulk_insert_mappings.assert_not_called()
        mock_session.commit.assert_not_called()

    def test_insertbulk_exception(self):
        """Test bulk insert with exception"""
        mock_session = MagicMock()
        mock_session.bulk_insert_mappings.side_effect = Exception("Bulk insert failed")

        db_utils = self.DBUtils(mock_session)
        bulk_data = [{"id": 1, "name": "test1"}]

        with pytest.raises(Exception, match="Bulk insert failed"):
            db_utils.insertbulk(DatabaseModel, bulk_data)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception

    def test_deleteall_success(self):
        """Test successful delete all operation"""
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query

        db_utils = self.DBUtils(mock_session)
        db_utils.deleteall(DatabaseModel)

        mock_session.query.assert_called_once_with(DatabaseModel)
        mock_query.delete.assert_called_once()
        mock_session.commit.assert_called_once()

    def test_deleteall_exception(self):
        """Test delete all with exception"""
        mock_session = MagicMock()
        mock_query = MagicMock()
        mock_query.delete.side_effect = Exception("Delete failed")
        mock_session.query.return_value = mock_query

        db_utils = self.DBUtils(mock_session)

        with pytest.raises(Exception, match="Delete failed"):
            db_utils.deleteall(DatabaseModel)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception

    def test_execute_success(self):
        """Test successful execute operation"""
        mock_session = MagicMock()

        db_utils = self.DBUtils(mock_session)
        stmt = sa.text("UPDATE test_model SET name = 'updated'")

        db_utils.execute(stmt)

        mock_session.execute.assert_called_once_with(stmt)
        mock_session.commit.assert_called_once()

    def test_execute_exception(self):
        """Test execute with exception"""
        mock_session = MagicMock()
        mock_session.execute.side_effect = Exception("Execute failed")

        db_utils = self.DBUtils(mock_session)
        stmt = sa.text("UPDATE test_model SET name = 'updated'")

        with pytest.raises(Exception, match="Execute failed"):
            db_utils.execute(stmt)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception


class TestDBUtilsAsync:
    """Test DBUtilsAsync class"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases import DBUtilsAsync

        self.DBUtilsAsync = DBUtilsAsync

    def test_init(self):
        """Test DBUtilsAsync initialization"""
        mock_session = MagicMock()
        db_utils = self.DBUtilsAsync(mock_session)
        assert db_utils.session is mock_session

    @pytest.mark.asyncio
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
        stmt = sa.select(DatabaseModel)
        result = await db_utils.fetchall(stmt)

        assert result == mock_result
        mock_session.execute.assert_called_once_with(stmt)
        mock_cursor.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetchall_as_dict(self):
        """Test async fetchall with as_dict=True"""
        mock_session = AsyncMock()
        mock_cursor = MagicMock()

        # Create mock Row objects that have _asdict method
        mock_row1 = MagicMock()
        mock_row1._asdict.return_value = {"id": 1, "name": "test1"}
        mock_row2 = MagicMock()
        mock_row2._asdict.return_value = {"id": 2, "name": "test2"}

        mock_session.execute.return_value = mock_cursor
        mock_cursor.all.return_value = [mock_row1, mock_row2]

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(DatabaseModel)
        result = await db_utils.fetchall(stmt, as_dict=True)

        assert result == [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
        mock_session.execute.assert_called_once_with(stmt)
        mock_cursor.all.assert_called_once()
        mock_cursor.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetchall_exception(self):
        """Test async fetchall with exception"""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Query failed")

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(DatabaseModel)

        with pytest.raises(Exception, match="Query failed"):
            await db_utils.fetchall(stmt)

        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetchvalue_success(self):
        """Test successful async fetchvalue operation"""
        mock_session = AsyncMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("test_value",)

        mock_session.execute.return_value = mock_cursor

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(DatabaseModel.name)
        result = await db_utils.fetchvalue(stmt)

        assert result == "test_value"
        mock_cursor.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetchvalue_exception(self):
        """Test async fetchvalue with exception"""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Query failed")

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.select(DatabaseModel.name)

        with pytest.raises(Exception, match="Query failed"):
            await db_utils.fetchvalue(stmt)

        mock_session.rollback.assert_called_once()

    @pytest.mark.asyncio
    async def test_insert_success(self):
        """Test successful async insert operation"""
        mock_session = AsyncMock()
        # Make add a regular (non-coroutine) method
        mock_session.add = MagicMock()

        db_utils = self.DBUtilsAsync(mock_session)
        test_obj = DatabaseModel(id=1, name="test")

        result = await db_utils.insert(test_obj)

        assert result is test_obj
        mock_session.add.assert_called_once_with(test_obj)
        mock_session.commit.assert_called_once()
        mock_session.refresh.assert_called_once_with(test_obj)

    @pytest.mark.asyncio
    async def test_insert_exception(self):
        """Test async insert with exception"""
        mock_session = AsyncMock()
        # Make add method raise exception when called (not async)
        mock_session.add = MagicMock(side_effect=Exception("Insert failed"))

        db_utils = self.DBUtilsAsync(mock_session)
        test_obj = DatabaseModel(id=1, name="test")

        with pytest.raises(Exception, match="Insert failed"):
            await db_utils.insert(test_obj)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception
        mock_session.refresh.assert_not_called()  # Should not refresh on exception

    @pytest.mark.asyncio
    async def test_deleteall_success(self):
        """Test successful async delete all operation"""
        mock_session = AsyncMock()

        db_utils = self.DBUtilsAsync(mock_session)

        await db_utils.deleteall(DatabaseModel)

        mock_session.execute.assert_called_once()
        # Check that delete statement was used
        call_args = mock_session.execute.call_args[0][0]
        assert hasattr(call_args, 'table')
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_deleteall_exception(self):
        """Test async delete all with exception"""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Delete failed")

        db_utils = self.DBUtilsAsync(mock_session)

        with pytest.raises(Exception, match="Delete failed"):
            await db_utils.deleteall(DatabaseModel)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception

    @pytest.mark.asyncio
    async def test_execute_success(self):
        """Test successful async execute operation"""
        mock_session = AsyncMock()

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.text("UPDATE test_model SET name = 'updated'")

        await db_utils.execute(stmt)

        mock_session.execute.assert_called_once_with(stmt)
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_exception(self):
        """Test async execute with exception"""
        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Execute failed")

        db_utils = self.DBUtilsAsync(mock_session)
        stmt = sa.text("UPDATE test_model SET name = 'updated'")

        with pytest.raises(Exception, match="Execute failed"):
            await db_utils.execute(stmt)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception


class TestBaseConnectionContextManagers:
    """Test BaseConnection context manager methods for increased coverage"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases.core.base import BaseConnection, ConnectionTester

        self.BaseConnection = BaseConnection
        self.ConnectionTester = ConnectionTester

    def test_sync_context_manager(self):
        """Test sync context manager __enter__ and __exit__ methods"""
        from ddcDatabases.core.configs import BaseRetryConfig

        connection_url = {"host": "localhost", "database": "test"}
        engine_args = {"echo": False}

        conn = ConcreteTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=True,
            expire_on_commit=False,
            sync_driver="postgresql+psycopg",
            async_driver=None,
            conn_retry_config=BaseRetryConfig(enable_retry=False),
        )

        mock_session = MagicMock()
        mock_engine = MagicMock()

        with (
            patch.object(conn, '_get_engine') as mock_get_engine,
            patch('ddcDatabases.core.base.sessionmaker') as mock_sessionmaker,
            patch.object(conn, '_test_connection_sync') as mock_test_conn,
        ):
            mock_get_engine.return_value.__enter__.return_value = mock_engine
            mock_get_engine.return_value.__exit__.return_value = None

            mock_session_maker = MagicMock()
            mock_session_maker.begin.return_value.__enter__.return_value = mock_session
            mock_session_maker.begin.return_value.__exit__.return_value = None
            mock_sessionmaker.return_value = mock_session_maker

            with conn as session:
                assert session is mock_session
                assert conn.is_connected == True
                mock_test_conn.assert_called_once_with(mock_session)

            # After exiting context, connection should be cleaned up
            assert conn.is_connected == False
            mock_session.close.assert_called_once()
            mock_engine.dispose.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test async context manager __aenter__ and __aexit__ methods - Lines 66-76, 79-83"""
        connection_url = {"host": "localhost", "database": "test"}
        engine_args = {"echo": False}

        conn = ConcreteTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=False,
            expire_on_commit=True,
            sync_driver=None,
            async_driver="postgresql+asyncpg",
        )

        mock_session = AsyncMock()
        mock_engine = AsyncMock()

        with (
            patch.object(conn, '_get_async_engine') as mock_get_engine,
            patch('ddcDatabases.core.base.async_sessionmaker') as mock_sessionmaker,
            patch.object(conn, '_test_connection_async') as mock_test_conn,
        ):

            mock_get_engine.return_value.__aenter__.return_value = mock_engine
            mock_get_engine.return_value.__aexit__.return_value = None

            mock_session_maker = MagicMock()
            mock_session_maker.begin.return_value.__aenter__.return_value = mock_session
            mock_session_maker.begin.return_value.__aexit__.return_value = None
            mock_sessionmaker.return_value = mock_session_maker

            async with conn as session:
                assert session is mock_session
                assert conn.is_connected == True
                mock_test_conn.assert_called_once_with(mock_session)

            # After exiting context, connection should be cleaned up
            assert conn.is_connected == False
            mock_session.close.assert_called_once()
            # Engine dispose is called once in __aexit__ (we mocked _get_async_engine)
            mock_engine.dispose.assert_called_once()

    @pytest.mark.skipif(not POSTGRESQL_AVAILABLE, reason="PostgreSQL drivers not available")
    @patch('sqlalchemy.engine.create_engine')
    def test_get_engine_context_manager(self, mock_create_engine):
        """Test _get_engine context manager - Lines 86-102"""
        connection_url = {"host": "localhost", "database": "test"}
        engine_args = {"echo": True, "pool_size": 5}

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        conn = ConcreteTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=True,
            expire_on_commit=False,
            sync_driver="postgresql+psycopg",
            async_driver=None,
        )

        with conn._get_engine() as engine:
            assert engine is mock_engine
            mock_create_engine.assert_called_once()

        mock_engine.dispose.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.skipif(not POSTGRESQL_AVAILABLE, reason="PostgreSQL drivers not available")
    async def test_get_async_engine_context_manager(self):
        """Test _get_async_engine context manager - Lines 105-119"""
        connection_url = {"host": "localhost", "database": "test"}
        engine_args = {"echo": False, "max_overflow": 15}

        conn = ConcreteTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args=engine_args,
            autoflush=True,
            expire_on_commit=False,
            sync_driver=None,
            async_driver="postgresql+asyncpg",
        )

        # Test the actual _get_async_engine method with real engine creation
        async with conn._get_async_engine() as engine:
            # Our concrete implementation creates real engines
            assert engine is not None
            # Check that the engine has the expected configuration
            assert hasattr(engine, 'dispose')  # Engine should have disposed method
            assert hasattr(engine, 'url')  # Should have URL attribute

    def test_test_connection_sync_method(self):
        """Test _test_connection_sync method - Lines 122-132"""
        connection_url = {"host": "localhost", "database": "test", "password": "secret"}

        conn = ConcreteTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args={},
            autoflush=True,
            expire_on_commit=False,
            sync_driver="postgresql+psycopg",
            async_driver=None,
        )

        mock_session = MagicMock()

        with patch('ddcDatabases.core.base.ConnectionTester') as mock_tester_class:
            mock_tester = MagicMock()
            mock_tester_class.return_value = mock_tester

            conn._test_connection_sync(mock_session)

            # Verify self.ConnectionTester was created with correct parameters
            mock_tester_class.assert_called_once()
            call_kwargs = mock_tester_class.call_args[1]

            assert call_kwargs['sync_session'] is mock_session
            assert isinstance(call_kwargs['host_url'], URL)
            # Verify password was removed from connection URL
            assert 'password' not in str(call_kwargs['host_url'])

            mock_tester.test_connection_sync.assert_called_once()

    @pytest.mark.asyncio
    async def test_test_connection_async_method(self):
        """Test _test_connection_async method - Lines 135-145"""
        connection_url = {"host": "localhost", "database": "test", "password": "secret"}

        conn = ConcreteTestConnection.create_test_connection(
            connection_url=connection_url,
            engine_args={},
            autoflush=True,
            expire_on_commit=False,
            sync_driver=None,
            async_driver="postgresql+asyncpg",
        )

        mock_session = AsyncMock()

        with patch('ddcDatabases.core.base.ConnectionTester') as mock_tester_class:
            mock_tester = MagicMock()
            mock_tester.test_connection_async = AsyncMock()
            mock_tester_class.return_value = mock_tester

            await conn._test_connection_async(mock_session)

            # Verify self.ConnectionTester was created with correct parameters
            mock_tester_class.assert_called_once()
            call_kwargs = mock_tester_class.call_args[1]

            assert call_kwargs['async_session'] is mock_session
            assert isinstance(call_kwargs['host_url'], URL)
            # Verify password was removed from connection URL
            assert 'password' not in str(call_kwargs['host_url'])

            mock_tester.test_connection_async.assert_called_once()


class TestConnectionTesterCoverage:
    """Test ConnectionTester methods for increased coverage"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases.core.base import ConnectionTester

        self.ConnectionTester = ConnectionTester

    def test_sync_oracle_connection(self):
        """Test Oracle-specific query in sync connection - Line 164"""
        mock_session = MagicMock()
        mock_session.bind.url = "oracle://user:password@host/xe"

        tester = self.ConnectionTester(sync_session=mock_session)
        result = tester.test_connection_sync()

        assert result == True
        mock_session.execute.assert_called_once()
        # Verify Oracle-specific query was used
        call_args = mock_session.execute.call_args[0][0]
        assert "SELECT 1 FROM dual" in str(call_args)

    @pytest.mark.asyncio
    async def test_async_oracle_connection(self):
        """Test Oracle-specific query in async connection - Lines 181, 185"""
        mock_session = AsyncMock()
        mock_session.bind.url = "oracle://user:password@host/xe"

        tester = self.ConnectionTester(async_session=mock_session)
        result = await tester.test_connection_async()

        assert result == True  # This tests line 185
        mock_session.execute.assert_called_once()
        # Verify Oracle-specific query was used (line 181)
        call_args = mock_session.execute.call_args[0][0]
        assert "SELECT 1 FROM dual" in str(call_args)


class TestDBUtilsAsyncInsertBulk:
    """Test DBUtilsAsync insertbulk method for increased coverage"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases import DBUtilsAsync

        self.DBUtilsAsync = DBUtilsAsync

    @pytest.mark.asyncio
    async def test_insertbulk_success(self):
        """Test successful async bulk insert"""
        mock_session = AsyncMock()
        mock_session.run_sync = AsyncMock()

        db_utils = self.DBUtilsAsync(mock_session)
        bulk_data = [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]

        result = await db_utils.insertbulk(DatabaseModel, bulk_data)

        # Verify the method returns None
        assert result is None

        # Verify session methods were called
        mock_session.run_sync.assert_called_once()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_insertbulk_empty_list(self):
        """Test async bulk insert with empty list"""
        mock_session = AsyncMock()
        mock_session.run_sync = AsyncMock()

        db_utils = self.DBUtilsAsync(mock_session)
        bulk_data = []

        result = await db_utils.insertbulk(DatabaseModel, bulk_data)

        # Verify the method returns None for empty list
        assert result is None

        # Verify no database operations were performed
        mock_session.run_sync.assert_not_called()
        mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_insertbulk_exception_handling(self):
        """Test async bulk insert exception handling"""
        mock_session = AsyncMock()
        # Make run_sync raise an exception
        mock_session.run_sync.side_effect = Exception("Bulk insert failed")

        db_utils = self.DBUtilsAsync(mock_session)
        bulk_data = [{"id": 1, "name": "test1"}]

        with pytest.raises(Exception, match="Bulk insert failed"):
            await db_utils.insertbulk(DatabaseModel, bulk_data)

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()  # Should not commit on exception
