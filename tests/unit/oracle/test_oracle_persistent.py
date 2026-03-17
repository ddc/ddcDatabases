"""Tests for Oracle persistent connections."""

# noinspection PyProtectedMember
from ddcdatabases.core.persistent import (
    OraclePersistent,
    PersistentSQLAlchemyConnection,
    close_all_persistent_connections,
)


class TestOraclePersistent:
    """Test Oracle persistent connection specifics."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_connection_creates_correct_url(self):
        """Test Oracle sync persistent connection URL formation."""
        conn = OraclePersistent(
            host="testhost",
            port=1522,
            user="testuser",
            password="testpass",
            servicename="testservice",
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "oracle" in conn.connection_key

    def test_oracle_only_supports_sync(self):
        """Test Oracle persistent connection only supports sync mode."""
        # Oracle doesn't have an async_mode parameter - it only supports sync
        conn = OraclePersistent(
            host="testhost",
            port=1521,
            user="testuser",
            password="testpass",
            servicename="testservice",
        )
        # Always returns sync connection
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "oracle" in conn.connection_key
        assert "testhost" in conn.connection_key

    def test_default_port(self):
        """Test Oracle default port is used when not specified."""
        conn = OraclePersistent(
            host="localhost",
            user="test",
            password="test",
            servicename="xe",
        )
        assert "1521" in conn.connection_key

    def test_singleton_pattern(self):
        """Test that same params return same instance."""
        conn1 = OraclePersistent(
            host="localhost",
            port=1521,
            user="test",
            password="test",
            servicename="singleton_test",
        )
        conn2 = OraclePersistent(
            host="localhost",
            port=1521,
            user="test",
            password="test",
            servicename="singleton_test",
        )
        assert conn1 is conn2

    def test_different_servicenames_return_different_instances(self):
        """Test that different service names return different instances."""
        conn1 = OraclePersistent(
            host="localhost",
            user="test",
            password="test",
            servicename="service1",
        )
        conn2 = OraclePersistent(
            host="localhost",
            user="test",
            password="test",
            servicename="service2",
        )
        assert conn1 is not conn2

    def test_connection_key_format(self):
        """Test that connection key has correct format."""
        conn = OraclePersistent(
            host="myhost",
            port=1521,
            user="myuser",
            password="mypass",
            servicename="myservice",
        )
        assert conn.connection_key == "oracle://myuser@myhost:1521/myservice"
