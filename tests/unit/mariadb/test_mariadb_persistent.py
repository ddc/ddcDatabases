"""Tests for MariaDB persistent connections (MySQL alias).

Note: MariaDB uses MySQL driver and persistent connections, these tests verify
the MariaDB aliases work correctly.
"""

# noinspection PyProtectedMember
from ddcdatabases import MariaDBPersistent
from ddcdatabases.core.persistent import (
    MySQLPersistent,
    PersistentSQLAlchemyAsyncConnection,
    PersistentSQLAlchemyConnection,
    close_all_persistent_connections,
)


class TestMariaDBPersistent:
    """Test MariaDB persistent connection specifics."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_mariadb_persistent_is_mysql_alias(self):
        """Test that MariaDBPersistent is an alias for MySQLPersistent."""
        assert MariaDBPersistent is MySQLPersistent

    def test_sync_connection_creates_correct_type(self):
        """Test MariaDB sync persistent connection returns correct type."""
        conn = MariaDBPersistent(
            host="testhost",
            port=3307,
            user="testuser",
            password="testpass",
            database="testdb",
            async_mode=False,
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "mysql" in conn.connection_key  # Uses MySQL driver

    def test_async_connection_creates_correct_type(self):
        """Test MariaDB async persistent connection returns correct type."""
        conn = MariaDBPersistent(
            host="testhost",
            port=3306,
            user="testuser",
            password="testpass",
            database="testdb",
            async_mode=True,
        )
        assert isinstance(conn, PersistentSQLAlchemyAsyncConnection)
        assert "mysql" in conn.connection_key  # Uses MySQL driver

    def test_default_port(self):
        """Test MariaDB default port is used when not specified."""
        conn = MariaDBPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
            async_mode=False,
        )
        assert "3306" in conn.connection_key

    def test_singleton_pattern(self):
        """Test that same params return same instance."""
        conn1 = MariaDBPersistent(
            host="localhost",
            port=3306,
            user="test",
            password="test",
            database="mariadb_singleton_test",
        )
        conn2 = MariaDBPersistent(
            host="localhost",
            port=3306,
            user="test",
            password="test",
            database="mariadb_singleton_test",
        )
        assert conn1 is conn2

    def test_different_databases_return_different_instances(self):
        """Test that different databases return different instances."""
        conn1 = MariaDBPersistent(
            host="localhost",
            user="test",
            password="test",
            database="mariadb_db1",
        )
        conn2 = MariaDBPersistent(
            host="localhost",
            user="test",
            password="test",
            database="mariadb_db2",
        )
        assert conn1 is not conn2

    def test_connection_key_format(self):
        """Test that connection key has correct format."""
        conn = MariaDBPersistent(
            host="myhost",
            port=3306,
            user="myuser",
            password="mypass",
            database="mydb",
        )
        # MariaDB uses MySQL driver, so key contains mysql
        assert conn.connection_key == "mysql://myuser@myhost:3306/mydb"
