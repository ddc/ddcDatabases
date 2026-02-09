"""Integration tests for PostgreSQL persistent connections."""

import pytest
import sqlalchemy as sa

# noinspection PyProtectedMember
from ddcDatabases.core.persistent import (
    PostgreSQLPersistent,
    close_all_persistent_connections,
)
from tests.integration.conftest import Base, IntegrationModel

pytestmark = pytest.mark.integration


class TestPostgreSQLPersistentIntegration:
    """Integration tests for PostgreSQL persistent connections."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_sync_persistent_connection(self, postgres_container):
        """Test synchronous PostgreSQL persistent connection."""
        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        conn = PostgreSQLPersistent(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            async_mode=False,
        )

        with conn as session:
            # Create tables
            Base.metadata.create_all(session.bind)

            # Insert
            stmt = sa.insert(IntegrationModel).values(name="persistent_test", enabled=True)
            session.execute(stmt)
            session.commit()

            # Verify
            stmt = sa.select(IntegrationModel.name).where(IntegrationModel.name == "persistent_test")
            result = session.execute(stmt).scalar()
            assert result == "persistent_test"

            # Cleanup
            stmt = sa.delete(IntegrationModel).where(IntegrationModel.name == "persistent_test")
            session.execute(stmt)
            session.commit()

        conn.shutdown()

    def test_persistent_connection_reuse(self, postgres_container):
        """Test that persistent connection reuses the same connection."""
        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        conn1 = PostgreSQLPersistent(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            async_mode=False,
        )
        conn2 = PostgreSQLPersistent(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            async_mode=False,
        )

        # Should be the same instance (singleton)
        assert conn1 is conn2

        with conn1 as session1:
            Base.metadata.create_all(session1.bind)
            result1 = session1.execute(sa.text("SELECT 1")).scalar()
            assert result1 == 1

        with conn2 as session2:
            result2 = session2.execute(sa.text("SELECT 2")).scalar()
            assert result2 == 2

        # Sessions should be the same (persistent)
        assert session1 is session2

        conn1.shutdown()

    def test_persistent_connection_multiple_operations(self, postgres_container):
        """Test multiple operations on persistent connection."""
        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        conn = PostgreSQLPersistent(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            async_mode=False,
        )

        # First use
        with conn as session:
            Base.metadata.create_all(session.bind)
            session.execute(sa.insert(IntegrationModel).values(name="op1", enabled=True))
            session.commit()

        # Second use
        with conn as session:
            session.execute(sa.insert(IntegrationModel).values(name="op2", enabled=False))
            session.commit()

        # Third use - verify both records exist
        with conn as session:
            stmt = sa.select(IntegrationModel.name).order_by(IntegrationModel.name)
            results = session.execute(stmt).scalars().all()
            assert "op1" in results
            assert "op2" in results

            # Cleanup
            session.execute(sa.delete(IntegrationModel))
            session.commit()

        conn.shutdown()

    @pytest.mark.asyncio
    async def test_async_persistent_connection(self, postgres_container):
        """Test asynchronous PostgreSQL persistent connection."""
        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        # Create tables with sync connection first
        sync_conn = PostgreSQLPersistent(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            async_mode=False,
        )
        with sync_conn as session:
            Base.metadata.create_all(session.bind)
        sync_conn.shutdown()

        # Clear registry to get fresh async connection
        close_all_persistent_connections()

        async_conn = PostgreSQLPersistent(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            async_mode=True,
        )

        async with async_conn as session:
            # Insert
            stmt = sa.insert(IntegrationModel).values(name="async_persistent", enabled=True)
            await session.execute(stmt)
            await session.commit()

            # Verify
            stmt = sa.select(IntegrationModel.name).where(IntegrationModel.name == "async_persistent")
            result = await session.execute(stmt)
            name = result.scalar()
            assert name == "async_persistent"

            # Cleanup
            stmt = sa.delete(IntegrationModel).where(IntegrationModel.name == "async_persistent")
            await session.execute(stmt)
            await session.commit()
