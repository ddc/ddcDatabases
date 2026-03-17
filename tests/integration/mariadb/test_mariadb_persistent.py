"""Integration tests for MariaDB persistent connections.

Note: MariaDB uses MySQL driver and persistent connections, these tests verify
the MariaDB aliases work correctly.
"""

import pytest
import sqlalchemy as sa

# noinspection PyProtectedMember
from ddcdatabases import MariaDBPersistent
from ddcdatabases.core.persistent import close_all_persistent_connections
from tests.integration.conftest import Base, IntegrationModel

pytestmark = pytest.mark.integration


class TestMariaDBPersistentIntegration:
    """Integration tests for MariaDB persistent connections."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_persistent_connection(self, mariadb_container):
        """Test synchronous MariaDB persistent connection."""
        port = mariadb_container.get_exposed_port(3306)
        host = mariadb_container.get_container_host_ip()

        conn = MariaDBPersistent(
            host=host,
            port=int(port),
            user=mariadb_container.username,
            password=mariadb_container.password,
            database=mariadb_container.dbname,
            async_mode=False,
        )

        with conn as session:
            # Create tables
            Base.metadata.create_all(session.bind)

            # Insert
            stmt = sa.insert(IntegrationModel).values(name="mariadb_persistent_test", enabled=True)
            session.execute(stmt)
            session.commit()

            # Verify
            stmt = sa.select(IntegrationModel.name).where(IntegrationModel.name == "mariadb_persistent_test")
            result = session.execute(stmt).scalar()
            assert result == "mariadb_persistent_test"

            # Cleanup
            stmt = sa.delete(IntegrationModel).where(IntegrationModel.name == "mariadb_persistent_test")
            session.execute(stmt)
            session.commit()

        conn.shutdown()

    def test_persistent_connection_singleton(self, mariadb_container):
        """Test MariaDB persistent connection singleton pattern."""
        port = mariadb_container.get_exposed_port(3306)
        host = mariadb_container.get_container_host_ip()

        conn1 = MariaDBPersistent(
            host=host,
            port=int(port),
            user=mariadb_container.username,
            password=mariadb_container.password,
            database=mariadb_container.dbname,
            async_mode=False,
        )
        conn2 = MariaDBPersistent(
            host=host,
            port=int(port),
            user=mariadb_container.username,
            password=mariadb_container.password,
            database=mariadb_container.dbname,
            async_mode=False,
        )

        assert conn1 is conn2

        conn1.shutdown()

    def test_persistent_connection_multiple_operations(self, mariadb_container):
        """Test multiple operations on MariaDB persistent connection."""
        port = mariadb_container.get_exposed_port(3306)
        host = mariadb_container.get_container_host_ip()

        conn = MariaDBPersistent(
            host=host,
            port=int(port),
            user=mariadb_container.username,
            password=mariadb_container.password,
            database=mariadb_container.dbname,
            async_mode=False,
        )

        # First use
        with conn as session:
            Base.metadata.create_all(session.bind)
            session.execute(sa.insert(IntegrationModel).values(name="mariadb_op1", enabled=True))
            session.commit()

        # Second use
        with conn as session:
            session.execute(sa.insert(IntegrationModel).values(name="mariadb_op2", enabled=False))
            session.commit()

        # Third use - verify both records exist
        with conn as session:
            stmt = sa.select(IntegrationModel.name).order_by(IntegrationModel.name)
            results = session.execute(stmt).scalars().all()
            assert "mariadb_op1" in results
            assert "mariadb_op2" in results

            # Cleanup
            session.execute(sa.delete(IntegrationModel))
            session.commit()

        conn.shutdown()
