"""Integration tests for MySQL persistent connections."""

import pytest
import sqlalchemy as sa

# noinspection PyProtectedMember
from ddcDatabases.core.persistent import (
    MySQLPersistent,
    close_all_persistent_connections,
)
from tests.integration.conftest import Base, IntegrationModel

pytestmark = pytest.mark.integration


class TestMySQLPersistentIntegration:
    """Integration tests for MySQL persistent connections."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_persistent_connection(self, mysql_container):
        """Test synchronous MySQL persistent connection."""
        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        conn = MySQLPersistent(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            async_mode=False,
        )

        with conn as session:
            # Create tables
            Base.metadata.create_all(session.bind)

            # Insert
            stmt = sa.insert(IntegrationModel).values(name="mysql_persistent_test", enabled=True)
            session.execute(stmt)
            session.commit()

            # Verify
            stmt = sa.select(IntegrationModel.name).where(IntegrationModel.name == "mysql_persistent_test")
            result = session.execute(stmt).scalar()
            assert result == "mysql_persistent_test"

            # Cleanup
            stmt = sa.delete(IntegrationModel).where(IntegrationModel.name == "mysql_persistent_test")
            session.execute(stmt)
            session.commit()

        conn.shutdown()

    def test_persistent_connection_singleton(self, mysql_container):
        """Test MySQL persistent connection singleton pattern."""
        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        conn1 = MySQLPersistent(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            async_mode=False,
        )
        conn2 = MySQLPersistent(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            async_mode=False,
        )

        assert conn1 is conn2

        conn1.shutdown()

    def test_persistent_connection_multiple_operations(self, mysql_container):
        """Test multiple operations on MySQL persistent connection."""
        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        conn = MySQLPersistent(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            async_mode=False,
        )

        # First use
        with conn as session:
            Base.metadata.create_all(session.bind)
            session.execute(sa.insert(IntegrationModel).values(name="mysql_op1", enabled=True))
            session.commit()

        # Second use
        with conn as session:
            session.execute(sa.insert(IntegrationModel).values(name="mysql_op2", enabled=False))
            session.commit()

        # Third use - verify both records exist
        with conn as session:
            stmt = sa.select(IntegrationModel.name).order_by(IntegrationModel.name)
            results = session.execute(stmt).scalars().all()
            assert "mysql_op1" in results
            assert "mysql_op2" in results

            # Cleanup
            session.execute(sa.delete(IntegrationModel))
            session.commit()

        conn.shutdown()
