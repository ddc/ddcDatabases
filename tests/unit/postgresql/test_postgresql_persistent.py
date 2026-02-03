"""Tests for PostgreSQL persistent connections."""

import pytest
from ddcDatabases.core.configs import BaseRetryConfig as RetryConfig

# noinspection PyProtectedMember
from ddcDatabases.core.persistent import (
    PersistentConnectionConfig,
    PersistentSQLAlchemyAsyncConnection,
    PersistentSQLAlchemyConnection,
    PostgreSQLPersistent,
    close_all_persistent_connections,
)
from unittest.mock import AsyncMock, MagicMock, patch

TEST_CONFIG = PersistentConnectionConfig(
    idle_timeout=300,
    health_check_interval=1,
    auto_reconnect=False,
)


class TestPostgreSQLPersistent:
    """Test PostgreSQL persistent connection specifics."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_connection_creates_correct_url(self):
        """Test PostgreSQL sync persistent connection URL formation."""
        conn = PostgreSQLPersistent(
            host="testhost",
            port=5433,
            user="testuser",
            password="testpass",
            database="testdb",
            async_mode=False,
        )
        assert isinstance(conn, PersistentSQLAlchemyConnection)
        assert "postgresql" in conn.connection_key
        assert "testhost" in conn.connection_key
        assert "5433" in conn.connection_key
        assert "testdb" in conn.connection_key

    def test_async_connection_creates_correct_url(self):
        """Test PostgreSQL async persistent connection URL formation."""
        conn = PostgreSQLPersistent(
            host="testhost",
            port=5433,
            user="testuser",
            password="testpass",
            database="testdb",
            async_mode=True,
        )
        assert isinstance(conn, PersistentSQLAlchemyAsyncConnection)
        assert "postgresql" in conn.connection_key
        assert "testhost" in conn.connection_key

    def test_default_port(self):
        """Test PostgreSQL default port is used when not specified."""
        conn = PostgreSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
            async_mode=False,
        )
        assert "5432" in conn.connection_key

    def test_custom_config(self):
        """Test PostgreSQL persistent with custom config."""
        config = PersistentConnectionConfig(idle_timeout=600)
        conn_retry_config = RetryConfig(max_retries=5)

        conn = PostgreSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
            config=config,
            connection_retry_config=conn_retry_config,
        )
        assert conn._config.idle_timeout == 600
        assert conn._connection_retry_config.max_retries == 5

    def test_singleton_pattern(self):
        """Test that same params return same instance."""
        conn1 = PostgreSQLPersistent(
            host="localhost",
            port=5432,
            user="test",
            password="test",
            database="singleton_test",
        )
        conn2 = PostgreSQLPersistent(
            host="localhost",
            port=5432,
            user="test",
            password="test",
            database="singleton_test",
        )
        assert conn1 is conn2

    def test_different_databases_return_different_instances(self):
        """Test that different databases return different instances."""
        conn1 = PostgreSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="db1",
        )
        conn2 = PostgreSQLPersistent(
            host="localhost",
            user="test",
            password="test",
            database="db2",
        )
        assert conn1 is not conn2

    def test_different_hosts_return_different_instances(self):
        """Test that different hosts return different instances."""
        conn1 = PostgreSQLPersistent(
            host="host1",
            user="test",
            password="test",
            database="testdb",
        )
        conn2 = PostgreSQLPersistent(
            host="host2",
            user="test",
            password="test",
            database="testdb",
        )
        assert conn1 is not conn2

    def test_context_manager_sync(self):
        """Test PostgreSQL sync persistent connection context manager."""
        conn = PersistentSQLAlchemyConnection(
            connection_key="postgresql://test@localhost:5432/testdb",
            connection_url="sqlite:///:memory:",
            config=TEST_CONFIG,
        )

        with conn as session:
            assert session is not None
            assert conn.is_connected

        conn.shutdown()

    @pytest.mark.asyncio
    async def test_context_manager_async(self):
        """Test PostgreSQL async persistent connection context manager."""
        mock_engine = MagicMock()
        mock_session = AsyncMock()
        mock_session.execute = AsyncMock()

        with patch.object(PersistentSQLAlchemyAsyncConnection, '_create_engine', return_value=mock_engine):
            with patch.object(PersistentSQLAlchemyAsyncConnection, '_create_session', return_value=mock_session):
                conn = PersistentSQLAlchemyAsyncConnection(
                    connection_key="postgresql://test@localhost:5432/testdb",
                    connection_url="postgresql+asyncpg://user:pass@localhost/db",
                    config=TEST_CONFIG,
                )

                async with conn as session:
                    assert session is mock_session
                    assert conn.is_connected

    def test_connection_key_format(self):
        """Test that connection key has correct format."""
        conn = PostgreSQLPersistent(
            host="myhost",
            port=5432,
            user="myuser",
            password="mypass",
            database="mydb",
        )
        assert conn.connection_key == "postgresql://myuser@myhost:5432/mydb"
