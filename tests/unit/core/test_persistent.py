"""Tests for persistent connection functionality."""

import pytest
import time
from ddcDatabases.core.configs import BaseRetryConfig as RetryConfig

# noinspection PyProtectedMember
from ddcDatabases.core.persistent import (
    MongoDBPersistent,
    MSSQLPersistent,
    MySQLPersistent,
    OraclePersistent,
    PersistentConnectionConfig,
    PersistentMongoDBConnection,
    PersistentSQLAlchemyAsyncConnection,
    PersistentSQLAlchemyConnection,
    PostgreSQLPersistent,
    _persistent_connections,
    _registry_lock,
    close_all_persistent_connections,
)
from sqlalchemy.exc import SQLAlchemyError
from unittest.mock import AsyncMock, MagicMock, patch

# Test-optimized configuration with short health check interval to speed up tests
TEST_CONFIG = PersistentConnectionConfig(
    idle_timeout=300,
    health_check_interval=1,  # 1 second instead of 30 for faster tests
    auto_reconnect=False,
)


class TestPersistentConnectionConfig:
    """Test PersistentConnectionConfig dataclass."""

    def test_default_values(self):
        """Test default configuration values."""
        config = PersistentConnectionConfig()
        assert config.idle_timeout == 300
        assert config.health_check_interval == 30
        assert config.auto_reconnect is True

    def test_custom_values(self):
        """Test custom configuration values."""
        config = PersistentConnectionConfig(
            idle_timeout=600,
            health_check_interval=60,
            auto_reconnect=False,
        )
        assert config.idle_timeout == 600
        assert config.health_check_interval == 60
        assert config.auto_reconnect is False

    def test_frozen_config(self):
        """Test that config is frozen (immutable)."""
        config = PersistentConnectionConfig()
        with pytest.raises(AttributeError):
            config.idle_timeout = 1000  # noqa


class TestPersistentSQLAlchemyConnection:
    """Test PersistentSQLAlchemyConnection class."""

    def test_initialization(self):
        """Test connection initialization."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql://user:pass@localhost/db",
        )
        assert conn.connection_key == "test://localhost/db"
        assert not conn.is_connected

    def test_custom_config(self):
        """Test connection with custom config."""
        config = PersistentConnectionConfig(idle_timeout=100)
        retry_config = RetryConfig(max_retries=5)

        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql://user:pass@localhost/db",
            config=config,
            retry_config=retry_config,
        )
        assert conn._config.idle_timeout == 100
        assert conn._retry_config.max_retries == 5

    def test_connect_creates_engine_and_session(self):
        """Test that connect creates engine and session."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",  # Use in-memory SQLite for testing
            config=TEST_CONFIG,
        )

        session = conn.connect()

        assert conn.is_connected
        assert session is not None
        assert conn._engine is not None
        assert conn._session is not None

        conn.shutdown()

    def test_disconnect(self):
        """Test disconnection."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql://user:pass@localhost/db",
        )
        conn._session = MagicMock()
        conn._engine = MagicMock()
        conn._is_connected = True

        conn.disconnect()

        assert not conn.is_connected
        assert conn._session is None
        assert conn._engine is None

    def test_context_manager(self):
        """Test context manager usage."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",  # Use in-memory SQLite for testing
            config=TEST_CONFIG,
        )

        with conn as session:
            assert session is not None
            assert conn.is_connected

        conn.shutdown()

    def test_shutdown(self):
        """Test shutdown method."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql://user:pass@localhost/db",
        )
        conn._session = MagicMock()
        conn._engine = MagicMock()
        conn._is_connected = True

        conn.shutdown()

        assert not conn.is_connected


class TestPersistentSQLAlchemyAsyncConnection:
    """Test PersistentSQLAlchemyAsyncConnection class."""

    def test_initialization(self):
        """Test async connection initialization."""
        conn = PersistentSQLAlchemyAsyncConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql+asyncpg://user:pass@localhost/db",
        )
        assert conn.connection_key == "test://localhost/db"
        assert not conn.is_connected

    def test_sync_connect_raises(self):
        """Test that sync connect raises NotImplementedError."""
        conn = PersistentSQLAlchemyAsyncConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql+asyncpg://user:pass@localhost/db",
        )
        with pytest.raises(NotImplementedError):
            conn.connect()

    def test_disconnect(self):
        """Test async disconnection."""
        conn = PersistentSQLAlchemyAsyncConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql+asyncpg://user:pass@localhost/db",
        )
        conn._session = MagicMock()
        conn._engine = MagicMock()
        conn._is_connected = True

        conn.disconnect()

        assert not conn.is_connected


class TestPersistentMongoDBConnection:
    """Test PersistentMongoDBConnection class."""

    def test_initialization(self):
        """Test MongoDB connection initialization."""
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
        )
        assert conn.connection_key == "mongodb://localhost/db"
        assert conn._database == "test_db"
        assert not conn.is_connected

    @patch('pymongo.MongoClient')
    def test_connect_success(self, mock_client_class):
        """Test successful MongoDB connection."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_class.return_value = mock_client

        # Disable auto_reconnect to avoid retry wrapper complications
        config = TEST_CONFIG
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
            config=config,
        )

        _ = conn.connect()

        assert conn.is_connected
        mock_client.admin.command.assert_called_with("ping")

        conn.shutdown()

    def test_disconnect(self):
        """Test MongoDB disconnection."""
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
        )
        conn._client = MagicMock()
        conn._db = MagicMock()
        conn._is_connected = True

        conn.disconnect()

        assert not conn.is_connected
        assert conn._client is None
        assert conn._db is None

    @patch('pymongo.MongoClient')
    def test_context_manager(self, mock_client_class):
        """Test MongoDB context manager usage."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_class.return_value = mock_client

        # Disable auto_reconnect to avoid retry wrapper complications
        config = TEST_CONFIG
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
            config=config,
        )

        with conn as _:
            assert conn.is_connected


class TestPersistentClasses:
    """Test persistent connection classes with singleton pattern."""

    # noinspection PyMethodMayBeStatic
    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    # noinspection PyMethodMayBeStatic
    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_postgresql_persistent_sync(self):
        """Test PostgreSQLPersistent sync mode."""
        conn = PostgreSQLPersistent(
            host="localhost",
            port=5432,
            user="test",
            password="test",
            database="testdb",
            async_mode=False,
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "postgresql" in conn.connection_key

    def test_postgresql_persistent_async(self):
        """Test PostgreSQLPersistent async mode."""
        conn = PostgreSQLPersistent(
            host="localhost",
            port=5432,
            user="test",
            password="test",
            database="testdb",
            async_mode=True,
        )
        assert isinstance(conn, PersistentSQLAlchemyAsyncConnection)

    def test_mysql_persistent_sync(self):
        """Test MySQLPersistent sync mode."""
        conn = MySQLPersistent(
            host="localhost",
            port=3306,
            user="test",
            password="test",
            database="testdb",
            async_mode=False,
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "mysql" in conn.connection_key

    def test_mysql_persistent_async(self):
        """Test MySQLPersistent async mode."""
        conn = MySQLPersistent(
            host="localhost",
            port=3306,
            user="test",
            password="test",
            database="testdb",
            async_mode=True,
        )
        assert isinstance(conn, PersistentSQLAlchemyAsyncConnection)

    def test_mssql_persistent_sync(self):
        """Test MSSQLPersistent sync mode."""
        conn = MSSQLPersistent(
            host="localhost",
            port=1433,
            user="test",
            password="test",
            database="testdb",
            async_mode=False,
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "mssql" in conn.connection_key

    def test_mssql_persistent_async(self):
        """Test MSSQLPersistent async mode."""
        conn = MSSQLPersistent(
            host="localhost",
            port=1433,
            user="test",
            password="test",
            database="testdb",
            async_mode=True,
        )
        assert isinstance(conn, PersistentSQLAlchemyAsyncConnection)

    def test_oracle_persistent(self):
        """Test OraclePersistent class."""
        conn = OraclePersistent(
            host="localhost",
            port=1521,
            user="test",
            password="test",
            servicename="xe",
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "oracle" in conn.connection_key

    def test_mongodb_persistent(self):
        """Test MongoDBPersistent class."""
        conn = MongoDBPersistent(
            host="localhost",
            port=27017,
            user="test",
            password="test",
            database="testdb",
        )
        assert isinstance(conn, PersistentMongoDBConnection)
        assert "mongodb" in conn.connection_key

    def test_singleton_pattern(self):
        """Test that same params return same instance."""
        conn1 = PostgreSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
        )
        conn2 = PostgreSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
        )
        assert conn1 is conn2

    def test_different_params_different_instance(self):
        """Test that different params return different instances."""
        conn1 = PostgreSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb1",
        )
        conn2 = PostgreSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb2",
        )
        assert conn1 is not conn2


class TestCloseAllPersistentConnections:
    """Test close_all_persistent_connections function."""

    # noinspection PyMethodMayBeStatic
    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    # noinspection PyMethodMayBeStatic
    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_close_all_connections(self):
        """Test closing all persistent connections."""
        # Create some connections
        PostgreSQLPersistent(host="localhost", user="test", password="test", database="db1")
        MySQLPersistent(host="localhost", user="test", password="test", database="db2")

        # Close all
        close_all_persistent_connections()

        # Registry should be empty
        with _registry_lock:
            assert len(_persistent_connections) == 0

    def test_close_with_no_connections(self):
        """Test that close_all works when registry is empty."""
        # First ensure registry is clear
        close_all_persistent_connections()

        # Should not raise
        close_all_persistent_connections()

        # Registry should still be empty
        with _registry_lock:
            assert len(_persistent_connections) == 0


class TestIdleCheckerFunctionality:
    """Test idle checker thread functionality."""

    def test_idle_checker_loop_disconnects_after_timeout(self):
        """Test that idle checker disconnects after idle timeout."""
        config = PersistentConnectionConfig(
            idle_timeout=1,  # 1 second timeout
            health_check_interval=1,  # Check every second
            auto_reconnect=False,
        )
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",
            config=config,
        )

        # Connect to start the idle checker
        conn.connect()
        assert conn.is_connected

        # Wait for idle timeout + health check interval + margin for CI
        time.sleep(3)

        # Should be disconnected due to idle timeout
        assert not conn.is_connected
        conn.shutdown()

    def test_idle_checker_thread_starts_correctly(self):
        """Test that idle checker thread starts with correct properties."""
        config = TEST_CONFIG
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",
            config=config,
        )

        conn.connect()

        # Verify thread exists and is daemon
        assert conn._idle_checker_thread is not None
        assert conn._idle_checker_thread.is_alive()
        assert conn._idle_checker_thread.daemon is True
        assert "idle-checker" in conn._idle_checker_thread.name

        conn.shutdown()

    def test_update_last_used_updates_timestamp(self):
        """Test that _update_last_used updates the timestamp."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",
        )

        old_time = conn._last_used
        time.sleep(0.01)
        conn._update_last_used()

        assert conn._last_used > old_time

    def test_shutdown_stops_idle_checker_thread(self):
        """Test that shutdown stops the idle checker thread."""
        config = TEST_CONFIG
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",
            config=config,
        )

        conn.connect()
        assert conn._idle_checker_thread.is_alive()

        conn.shutdown()

        # Give thread time to stop
        time.sleep(0.2)
        assert not conn._idle_checker_thread.is_alive()


class TestPersistentSQLAlchemyAsyncConnectionAsync:
    """Test async methods of PersistentSQLAlchemyAsyncConnection."""

    @pytest.mark.asyncio
    async def test_async_connect_success(self):
        """Test successful async connection."""
        config = TEST_CONFIG

        # Mock the engine and session creation at class level
        mock_engine = MagicMock()
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()

        with patch.object(PersistentSQLAlchemyAsyncConnection, '_create_engine', return_value=mock_engine):
            with patch.object(PersistentSQLAlchemyAsyncConnection, '_create_session', return_value=mock_session):
                conn = PersistentSQLAlchemyAsyncConnection(
                    connection_key="test://localhost/db",
                    connection_url="postgresql+asyncpg://user:pass@localhost/db",
                    config=config,
                )
                session = await conn.async_connect()

                assert conn.is_connected
                assert session is mock_session

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test async context manager usage."""
        config = TEST_CONFIG

        mock_engine = MagicMock()
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()

        with patch.object(PersistentSQLAlchemyAsyncConnection, '_create_engine', return_value=mock_engine):
            with patch.object(PersistentSQLAlchemyAsyncConnection, '_create_session', return_value=mock_session):
                conn = PersistentSQLAlchemyAsyncConnection(
                    connection_key="test://localhost/db",
                    connection_url="postgresql+asyncpg://user:pass@localhost/db",
                    config=config,
                )
                async with conn as session:
                    assert conn.is_connected
                    assert session is mock_session

    @pytest.mark.asyncio
    async def test_async_disconnect(self):
        """Test async disconnect."""
        conn = PersistentSQLAlchemyAsyncConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql+asyncpg://user:pass@localhost/db",
        )

        # Setup mocked connected state
        mock_session = AsyncMock()
        mock_engine = AsyncMock()
        conn._session = mock_session
        conn._engine = mock_engine
        conn._is_connected = True

        await conn.async_disconnect()
        assert not conn.is_connected

    @pytest.mark.asyncio
    async def test_async_disconnect_internal_with_errors(self):
        """Test _async_disconnect_internal handles errors gracefully."""
        conn = PersistentSQLAlchemyAsyncConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql+asyncpg://user:pass@localhost/db",
        )

        # Mock session and engine that raise errors on close
        mock_session = AsyncMock()
        mock_session.close.side_effect = Exception("Session close error")
        mock_engine = AsyncMock()
        mock_engine.dispose.side_effect = Exception("Engine dispose error")

        conn._session = mock_session
        conn._engine = mock_engine
        conn._is_connected = True

        # Should not raise, just log warnings
        await conn._async_disconnect_internal()

        assert not conn.is_connected
        assert conn._session is None
        assert conn._engine is None

    @pytest.mark.asyncio
    async def test_async_connect_reconnects_on_lost_connection(self):
        """Test that async_connect reconnects when connection is lost."""
        config = TEST_CONFIG

        # Mock for reconnection
        mock_engine = MagicMock()
        mock_session2 = AsyncMock()
        mock_session2.execute = AsyncMock()

        with patch.object(PersistentSQLAlchemyAsyncConnection, '_create_engine', return_value=mock_engine):
            with patch.object(PersistentSQLAlchemyAsyncConnection, '_create_session', return_value=mock_session2):
                conn = PersistentSQLAlchemyAsyncConnection(
                    connection_key="test://localhost/db",
                    connection_url="postgresql+asyncpg://user:pass@localhost/db",
                    config=config,
                )

                # Setup initial connected state with failing session
                mock_session1 = AsyncMock()
                mock_session1.execute.side_effect = SQLAlchemyError("Connection lost")
                conn._session = mock_session1
                conn._is_connected = True

                # This should detect failed connection and reconnect
                session = await conn.async_connect()
                assert conn.is_connected
                assert session is mock_session2


class TestPersistentMongoDBConnectionAdvanced:
    """Test advanced PersistentMongoDBConnection scenarios."""

    @patch('pymongo.MongoClient')
    def test_connect_reconnects_on_ping_failure(self, mock_client_class):
        """Test that connect reconnects when ping fails."""
        from pymongo.errors import PyMongoError

        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)

        # First call to ping fails, second succeeds
        mock_client.admin.command.side_effect = [PyMongoError("ping failed"), None, None]
        mock_client_class.return_value = mock_client

        config = TEST_CONFIG
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
            config=config,
        )

        # Manually set as connected to test reconnection path
        conn._client = mock_client
        conn._db = mock_db
        conn._is_connected = True

        # This should detect ping failure and reconnect
        _ = conn.connect()

        assert conn.is_connected
        # Should have called ping at least twice (one fail, one success)
        assert mock_client.admin.command.call_count >= 2

        conn.shutdown()

    def test_disconnect_internal_handles_close_error(self):
        """Test _disconnect_internal handles client close errors."""
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
        )

        mock_client = MagicMock()
        mock_client.close.side_effect = Exception("Close error")
        conn._client = mock_client
        conn._db = MagicMock()
        conn._is_connected = True

        # Should not raise, just log warning
        conn._disconnect_internal()

        assert not conn.is_connected
        assert conn._client is None


class TestPersistentSQLAlchemyConnectionAdvanced:
    """Test advanced PersistentSQLAlchemyConnection scenarios."""

    def test_disconnect_internal_handles_session_close_error(self):
        """Test _disconnect_internal handles session close errors."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql://user:pass@localhost/db",
        )

        mock_session = MagicMock()
        mock_session.close.side_effect = Exception("Session close error")
        mock_engine = MagicMock()

        conn._session = mock_session
        conn._engine = mock_engine
        conn._is_connected = True

        # Should not raise, just log warning
        conn._disconnect_internal()

        assert not conn.is_connected
        assert conn._session is None

    def test_disconnect_internal_handles_engine_dispose_error(self):
        """Test _disconnect_internal handles engine dispose errors."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="postgresql://user:pass@localhost/db",
        )

        mock_session = MagicMock()
        mock_engine = MagicMock()
        mock_engine.dispose.side_effect = Exception("Engine dispose error")

        conn._session = mock_session
        conn._engine = mock_engine
        conn._is_connected = True

        # Should not raise, just log warning
        conn._disconnect_internal()

        assert not conn.is_connected
        assert conn._engine is None

    def test_connect_reconnects_on_lost_connection(self):
        """Test that connect reconnects when SELECT 1 fails."""
        config = TEST_CONFIG
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",
            config=config,
        )

        # First connect
        session1 = conn.connect()
        assert conn.is_connected

        # Simulate connection loss
        with patch.object(session1, 'execute', side_effect=SQLAlchemyError("Connection lost")):
            # This should trigger reconnection
            session2 = conn.connect()
            assert conn.is_connected
            assert session2 is not None

        conn.shutdown()


class TestRetrySettingsIntegration:
    """Test retry settings in settings classes."""

    def test_postgresql_retry_settings(self):
        """Test PostgreSQL settings include retry fields."""
        from ddcDatabases.core.settings import PostgreSQLSettings

        settings = PostgreSQLSettings()
        assert hasattr(settings, 'conn_enable_retry')
        assert hasattr(settings, 'conn_max_retries')
        assert hasattr(settings, 'conn_initial_retry_delay')
        assert hasattr(settings, 'conn_max_retry_delay')
        assert hasattr(settings, 'op_enable_retry')
        assert hasattr(settings, 'op_max_retries')
        assert hasattr(settings, 'op_initial_retry_delay')
        assert hasattr(settings, 'op_max_retry_delay')
        assert hasattr(settings, 'op_jitter')
        assert hasattr(settings, 'conn_disconnect_idle_timeout')

        assert settings.conn_enable_retry is True
        assert settings.conn_max_retries == 3
        assert settings.conn_initial_retry_delay == pytest.approx(1.0)
        assert settings.conn_max_retry_delay == pytest.approx(30.0)
        assert settings.op_enable_retry is True
        assert settings.op_max_retries == 3
        assert settings.op_initial_retry_delay == pytest.approx(0.5)
        assert settings.op_max_retry_delay == pytest.approx(10.0)
        assert settings.op_jitter == pytest.approx(0.1)
        assert settings.conn_disconnect_idle_timeout == 300

    def test_mysql_retry_settings(self):
        """Test MySQL settings include retry fields."""
        from ddcDatabases.core.settings import MySQLSettings

        settings = MySQLSettings()
        assert settings.conn_enable_retry is True
        assert settings.conn_max_retries == 3
        assert settings.op_enable_retry is True
        assert settings.op_max_retries == 3
        assert settings.conn_disconnect_idle_timeout == 300

    def test_mssql_retry_settings(self):
        """Test MSSQL settings include retry fields."""
        from ddcDatabases.core.settings import MSSQLSettings

        settings = MSSQLSettings()
        assert settings.conn_enable_retry is True
        assert settings.conn_max_retries == 3
        assert settings.op_enable_retry is True
        assert settings.op_max_retries == 3
        assert settings.conn_disconnect_idle_timeout == 300

    def test_oracle_retry_settings(self):
        """Test Oracle settings include retry fields."""
        from ddcDatabases.core.settings import OracleSettings

        settings = OracleSettings()
        assert settings.conn_enable_retry is True
        assert settings.conn_max_retries == 3
        assert settings.op_enable_retry is True
        assert settings.op_max_retries == 3
        assert settings.conn_disconnect_idle_timeout == 300

    def test_mongodb_retry_settings(self):
        """Test MongoDB settings include retry fields."""
        from ddcDatabases.core.settings import MongoDBSettings

        settings = MongoDBSettings()
        assert settings.conn_enable_retry is True
        assert settings.conn_max_retries == 3
        assert settings.op_enable_retry is True
        assert settings.op_max_retries == 3
        assert settings.conn_disconnect_idle_timeout == 300

    def test_sqlite_retry_settings(self):
        """Test SQLite settings include retry fields (minimal)."""
        from ddcDatabases.core.settings import SQLiteSettings

        settings = SQLiteSettings()
        assert settings.conn_enable_retry is False  # SQLite disabled by default
        assert settings.conn_max_retries == 1  # Minimal retries for file-based DB
        assert settings.op_enable_retry is False  # SQLite disabled by default
        assert settings.op_max_retries == 1  # Minimal retries for file-based DB
