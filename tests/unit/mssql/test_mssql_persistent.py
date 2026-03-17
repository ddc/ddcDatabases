"""Tests for MSSQL persistent connections."""

# noinspection PyProtectedMember
from ddcdatabases.core.persistent import (
    MSSQLPersistent,
    PersistentSQLAlchemyAsyncConnection,
    PersistentSQLAlchemyConnection,
    close_all_persistent_connections,
)


class TestMSSQLPersistent:
    """Test MSSQL persistent connection specifics."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_connection_creates_correct_url(self):
        """Test MSSQL sync persistent connection URL formation."""
        conn = MSSQLPersistent(
            host="testhost",
            port=1434,
            user="testuser",
            password="testpass",
            database="testdb",
            async_mode=False,
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "mssql" in conn.connection_key

    def test_async_connection_creates_correct_url(self):
        """Test MSSQL async persistent connection URL formation."""
        conn = MSSQLPersistent(
            host="testhost",
            port=1433,
            user="testuser",
            password="testpass",
            database="testdb",
            async_mode=True,
        )
        assert isinstance(conn, PersistentSQLAlchemyAsyncConnection)
        assert "mssql" in conn.connection_key
        assert "testhost" in conn.connection_key

    def test_default_port(self):
        """Test MSSQL default port is used when not specified."""
        conn = MSSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
            async_mode=False,
        )
        assert "1433" in conn.connection_key

    def test_singleton_pattern(self):
        """Test that same params return same instance."""
        conn1 = MSSQLPersistent(
            host="localhost",
            port=1433,
            user="test",
            password="test",
            database="singleton_test",
        )
        conn2 = MSSQLPersistent(
            host="localhost",
            port=1433,
            user="test",
            password="test",
            database="singleton_test",
        )
        assert conn1 is conn2

    def test_different_databases_return_different_instances(self):
        """Test that different databases return different instances."""
        conn1 = MSSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="db1",
        )
        conn2 = MSSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="db2",
        )
        assert conn1 is not conn2

    def test_connection_key_format(self):
        """Test that connection key has correct format."""
        conn = MSSQLPersistent(
            host="myhost",
            port=1433,
            user="myuser",
            password="mypass",
            database="mydb",
        )
        assert conn.connection_key == "mssql://myuser@myhost:1433/mydb"
