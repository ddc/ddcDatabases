"""Tests for persistent connection functionality."""

import asyncio
import threading
import time
from unittest.mock import MagicMock, patch, AsyncMock
import pytest
from ddcDatabases.persistent import (
    PersistentConnectionConfig,
    PersistentSQLAlchemyConnection,
    PersistentSQLAlchemyAsyncConnection,
    PersistentMongoDBConnection,
    PostgreSQLPersistent,
    MySQLPersistent,
    MSSQLPersistent,
    OraclePersistent,
    MongoDBPersistent,
    close_all_persistent_connections,
    _persistent_connections,
    _registry_lock,
)
from ddcDatabases.db_utils import RetryConfig


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
            config.idle_timeout = 1000


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
        config = PersistentConnectionConfig(auto_reconnect=False)
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",  # Use in-memory SQLite for testing
            config=config,
        )

        session = conn.connect()

        assert conn.is_connected
        assert session is not None
        assert conn._engine is not None
        assert conn._session is not None

        conn.disconnect()

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
        config = PersistentConnectionConfig(auto_reconnect=False)
        conn = PersistentSQLAlchemyConnection(
            connection_key="test://localhost/db",
            connection_url="sqlite:///:memory:",  # Use in-memory SQLite for testing
            config=config,
        )

        with conn as session:
            assert session is not None
            assert conn.is_connected

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
        config = PersistentConnectionConfig(auto_reconnect=False)
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
            config=config,
        )

        db = conn.connect()

        assert conn.is_connected
        mock_client.admin.command.assert_called_with("ping")

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
        config = PersistentConnectionConfig(auto_reconnect=False)
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
            config=config,
        )

        with conn as db:
            assert conn.is_connected


class TestPersistentClasses:
    """Test persistent connection classes with singleton pattern."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

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

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

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


class TestRetrySettingsIntegration:
    """Test retry settings in settings classes."""

    def test_postgresql_retry_settings(self):
        """Test PostgreSQL settings include retry fields."""
        from ddcDatabases.settings import PostgreSQLSettings

        settings = PostgreSQLSettings()
        assert hasattr(settings, 'enable_retry')
        assert hasattr(settings, 'max_retries')
        assert hasattr(settings, 'initial_retry_delay')
        assert hasattr(settings, 'max_retry_delay')
        assert hasattr(settings, 'disconnect_idle_timeout')

        assert settings.enable_retry is True
        assert settings.max_retries == 3
        assert settings.initial_retry_delay == pytest.approx(1.0)
        assert settings.max_retry_delay == pytest.approx(30.0)
        assert settings.disconnect_idle_timeout == 300

    def test_mysql_retry_settings(self):
        """Test MySQL settings include retry fields."""
        from ddcDatabases.settings import MySQLSettings

        settings = MySQLSettings()
        assert settings.enable_retry is True
        assert settings.max_retries == 3
        assert settings.disconnect_idle_timeout == 300

    def test_mssql_retry_settings(self):
        """Test MSSQL settings include retry fields."""
        from ddcDatabases.settings import MSSQLSettings

        settings = MSSQLSettings()
        assert settings.enable_retry is True
        assert settings.max_retries == 3
        assert settings.disconnect_idle_timeout == 300

    def test_oracle_retry_settings(self):
        """Test Oracle settings include retry fields."""
        from ddcDatabases.settings import OracleSettings

        settings = OracleSettings()
        assert settings.enable_retry is True
        assert settings.max_retries == 3
        assert settings.disconnect_idle_timeout == 300

    def test_mongodb_retry_settings(self):
        """Test MongoDB settings include retry fields."""
        from ddcDatabases.settings import MongoDBSettings

        settings = MongoDBSettings()
        assert settings.enable_retry is True
        assert settings.max_retries == 3
        assert settings.disconnect_idle_timeout == 300

    def test_sqlite_retry_settings(self):
        """Test SQLite settings include retry fields (minimal)."""
        from ddcDatabases.settings import SQLiteSettings

        settings = SQLiteSettings()
        assert settings.enable_retry is False  # SQLite disabled by default
        assert settings.max_retries == 1  # Minimal retries for file-based DB
