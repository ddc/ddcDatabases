"""Integration tests for MSSQL persistent connections."""

import pytest
import sqlalchemy as sa

# noinspection PyProtectedMember
from ddcdatabases.core.persistent import (
    MSSQLPersistent,
    close_all_persistent_connections,
)

pytestmark = pytest.mark.integration


class TestMSSQLPersistentIntegration:
    """Integration tests for MSSQL persistent connections."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_persistent_connection(self, mssql_container):
        """Test synchronous MSSQL persistent connection."""
        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        conn = MSSQLPersistent(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="master",
            async_mode=False,
        )

        with conn as session:
            # Insert
            result = session.execute(sa.text("SELECT 1"))
            assert result.scalar() == 1

        conn.shutdown()

    def test_persistent_connection_singleton(self, mssql_container):
        """Test MSSQL persistent connection singleton pattern."""
        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        conn1 = MSSQLPersistent(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="master",
            async_mode=False,
        )
        conn2 = MSSQLPersistent(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="master",
            async_mode=False,
        )

        assert conn1 is conn2

        conn1.shutdown()

    def test_persistent_connection_multiple_operations(self, mssql_container):
        """Test multiple operations on MSSQL persistent connection."""
        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        conn = MSSQLPersistent(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="master",
            async_mode=False,
        )

        # First use
        with conn as session:
            result1 = session.execute(sa.text("SELECT 1")).scalar()
            assert result1 == 1

        # Second use
        with conn as session:
            result2 = session.execute(sa.text("SELECT 2")).scalar()
            assert result2 == 2

        conn.shutdown()
