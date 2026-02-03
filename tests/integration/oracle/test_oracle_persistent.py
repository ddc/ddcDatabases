"""Integration tests for Oracle persistent connections."""

import pytest
import sqlalchemy as sa

# noinspection PyProtectedMember
from ddcDatabases.core.persistent import (
    OraclePersistent,
    close_all_persistent_connections,
)

pytestmark = pytest.mark.integration


class TestOraclePersistentIntegration:
    """Integration tests for Oracle persistent connections."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_persistent_connection(self, oracle_container):
        """Test synchronous Oracle persistent connection."""
        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        conn = OraclePersistent(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
        )

        with conn as session:
            result = session.execute(sa.text("SELECT 1 FROM dual"))
            assert result.scalar() == 1

        conn.shutdown()

    def test_persistent_connection_singleton(self, oracle_container):
        """Test Oracle persistent connection singleton pattern."""
        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        conn1 = OraclePersistent(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
        )
        conn2 = OraclePersistent(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
        )

        assert conn1 is conn2

        conn1.shutdown()

    def test_persistent_connection_multiple_operations(self, oracle_container):
        """Test multiple operations on Oracle persistent connection."""
        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        conn = OraclePersistent(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
        )

        # First use
        with conn as session:
            result1 = session.execute(sa.text("SELECT 1 FROM dual")).scalar()
            assert result1 == 1

        # Second use
        with conn as session:
            result2 = session.execute(sa.text("SELECT 2 FROM dual")).scalar()
            assert result2 == 2

        conn.shutdown()
