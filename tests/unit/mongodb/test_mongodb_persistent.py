"""Tests for MongoDB persistent connections."""

# noinspection PyProtectedMember
from ddcDatabases.core.persistent import (
    MongoDBPersistent,
    PersistentConnectionConfig,
    PersistentMongoDBConnection,
    close_all_persistent_connections,
)
from unittest.mock import MagicMock, patch

TEST_CONFIG = PersistentConnectionConfig(
    idle_timeout=300,
    health_check_interval=1,
    auto_reconnect=False,
)


class TestMongoDBPersistent:
    """Test MongoDB persistent connection specifics."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_connection_creates_correct_url(self):
        """Test MongoDB persistent connection URL formation."""
        conn = MongoDBPersistent(
            host="testhost",
            port=27018,
            user="testuser",
            password="testpass",
            database="testdb",
        )
        assert isinstance(conn, PersistentMongoDBConnection)
        assert "mongodb" in conn.connection_key
        assert "testhost" in conn.connection_key
        assert "27018" in conn.connection_key

    def test_default_port(self):
        """Test MongoDB default port is used when not specified."""
        conn = MongoDBPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
        )
        assert "27017" in conn.connection_key

    def test_database_stored(self):
        """Test MongoDB database is correctly stored."""
        conn = MongoDBPersistent(
            host="localhost",
            user="test",
            password="test",
            database="mydb",
        )
        assert conn._database == "mydb"

    def test_singleton_pattern(self):
        """Test that same params return same instance."""
        conn1 = MongoDBPersistent(
            host="localhost",
            port=27017,
            user="test",
            password="test",
            database="singleton_test",
        )
        conn2 = MongoDBPersistent(
            host="localhost",
            port=27017,
            user="test",
            password="test",
            database="singleton_test",
        )
        assert conn1 is conn2

    def test_different_databases_return_different_instances(self):
        """Test that different databases return different instances."""
        conn1 = MongoDBPersistent(
            host="localhost",
            user="test",
            password="test",
            database="db1",
        )
        conn2 = MongoDBPersistent(
            host="localhost",
            user="test",
            password="test",
            database="db2",
        )
        assert conn1 is not conn2

    @patch('pymongo.MongoClient')
    def test_context_manager(self, mock_client_class):
        """Test MongoDB persistent connection context manager."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_class.return_value = mock_client

        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
            config=TEST_CONFIG,
        )

        with conn as db:
            assert conn.is_connected
            mock_client.admin.command.assert_called_with("ping")

        conn.shutdown()

    @patch('pymongo.MongoClient')
    def test_health_check_on_connect(self, mock_client_class):
        """Test that MongoDB health check (ping) runs on connect."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_client.__getitem__ = MagicMock(return_value=mock_db)
        mock_client_class.return_value = mock_client

        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
            config=TEST_CONFIG,
        )

        conn.connect()

        mock_client.admin.command.assert_called_with("ping")

        conn.shutdown()

    def test_connection_has_lock(self):
        """Test that MongoDB persistent connections have a lock."""
        conn = PersistentMongoDBConnection(
            connection_key="mongodb://localhost/db",
            connection_url="mongodb://user:pass@localhost/admin",
            database="test_db",
        )
        assert hasattr(conn, '_lock')

    def test_connection_key_format(self):
        """Test that connection key has correct format."""
        conn = MongoDBPersistent(
            host="myhost",
            port=27017,
            user="myuser",
            password="mypass",
            database="mydb",
        )
        assert conn.connection_key == "mongodb://myuser@myhost:27017/mydb"
