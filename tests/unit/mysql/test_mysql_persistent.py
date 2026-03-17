"""Tests for MySQL persistent connections."""

from ddcdatabases.core.configs import BaseRetryConfig as RetryConfig

# noinspection PyProtectedMember
from ddcdatabases.core.persistent import (
    MySQLPersistent,
    PersistentConnectionConfig,
    PersistentSQLAlchemyAsyncConnection,
    PersistentSQLAlchemyConnection,
    close_all_persistent_connections,
)

TEST_CONFIG = PersistentConnectionConfig(
    idle_timeout=300,
    health_check_interval=1,
    auto_reconnect=False,
)


class TestMySQLPersistent:
    """Test MySQL persistent connection specifics."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_connection_creates_correct_url(self):
        """Test MySQL sync persistent connection URL formation."""
        conn = MySQLPersistent(
            host="testhost",
            port=3307,
            user="testuser",
            password="testpass",
            database="testdb",
            async_mode=False,
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "mysql" in conn.connection_key
        assert "testhost" in conn.connection_key
        assert "3307" in conn.connection_key

    def test_async_connection_creates_correct_url(self):
        """Test MySQL async persistent connection URL formation."""
        conn = MySQLPersistent(
            host="testhost",
            port=3306,
            user="testuser",
            password="testpass",
            database="testdb",
            async_mode=True,
        )
        assert isinstance(conn, PersistentSQLAlchemyAsyncConnection)
        assert "mysql" in conn.connection_key
        assert "testhost" in conn.connection_key

    def test_default_port(self):
        """Test MySQL default port is used when not specified."""
        conn = MySQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
            async_mode=False,
        )
        assert "3306" in conn.connection_key

    def test_localhost_normalization(self):
        """Test that localhost is kept as-is for MySQL persistent connections."""
        conn = MySQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
            async_mode=False,
        )
        assert "localhost" in conn.connection_key or "127.0.0.1" in conn.connection_key

    def test_singleton_pattern(self):
        """Test that same params return same instance."""
        conn1 = MySQLPersistent(
            host="localhost",
            port=3306,
            user="test",
            password="test",
            database="singleton_test",
        )
        conn2 = MySQLPersistent(
            host="localhost",
            port=3306,
            user="test",
            password="test",
            database="singleton_test",
        )
        assert conn1 is conn2

    def test_different_databases_return_different_instances(self):
        """Test that different databases return different instances."""
        conn1 = MySQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="db1",
        )
        conn2 = MySQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="db2",
        )
        assert conn1 is not conn2

    def test_custom_config(self):
        """Test MySQL persistent with custom config."""
        config = PersistentConnectionConfig(idle_timeout=600)
        conn_retry_config = RetryConfig(max_retries=5)

        conn = MySQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
            config=config,
            connection_retry_config=conn_retry_config,
        )
        assert conn._config.idle_timeout == 600
        assert conn._connection_retry_config.max_retries == 5

    def test_connection_key_format(self):
        """Test that connection key has correct format."""
        conn = MySQLPersistent(
            host="myhost",
            port=3306,
            user="myuser",
            password="mypass",
            database="mydb",
        )
        assert conn.connection_key == "mysql://myuser@myhost:3306/mydb"
