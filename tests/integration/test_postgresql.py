import pytest
import sqlalchemy as sa
from tests.integration.conftest import Base, IntegrationModel

pytestmark = pytest.mark.integration


class TestPostgreSQLIntegration:
    """Integration tests for PostgreSQL using Testcontainers."""

    def test_sync_connection(self, postgres_container):
        """Test synchronous PostgreSQL connection and CRUD operations."""
        from ddcDatabases import DBUtils, PostgreSQL

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        ) as session:
            # Create tables
            Base.metadata.create_all(session.bind)

            db_utils = DBUtils(session)

            # Insert
            stmt = sa.insert(IntegrationModel).values(name="test_sync", enabled=True)
            db_utils.execute(stmt)

            # Select
            stmt = sa.select(IntegrationModel.name, IntegrationModel.enabled).where(
                IntegrationModel.name == "test_sync"
            )
            results = db_utils.fetchall(stmt, as_dict=True)
            assert len(results) == 1
            assert results[0]["name"] == "test_sync"
            assert results[0]["enabled"] is True

            # Update
            stmt = sa.update(IntegrationModel).where(IntegrationModel.name == "test_sync").values(enabled=False)
            db_utils.execute(stmt)

            stmt = sa.select(IntegrationModel.name, IntegrationModel.enabled).where(
                IntegrationModel.name == "test_sync"
            )
            results = db_utils.fetchall(stmt, as_dict=True)
            assert results[0]["enabled"] is False

            # Delete
            stmt = sa.delete(IntegrationModel).where(IntegrationModel.name == "test_sync")
            db_utils.execute(stmt)

            stmt = sa.select(IntegrationModel.id)
            results = db_utils.fetchall(stmt, as_dict=True)
            assert len(results) == 0

    @pytest.mark.asyncio
    async def test_async_connection(self, postgres_container):
        """Test asynchronous PostgreSQL connection and CRUD operations."""
        from ddcDatabases import DBUtilsAsync, PostgreSQL

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        # First create tables using sync connection
        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        ) as session:
            Base.metadata.create_all(session.bind)

        # Now test async
        async with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        ) as session:
            db_utils = DBUtilsAsync(session)

            # Insert
            stmt = sa.insert(IntegrationModel).values(name="test_async", enabled=True)
            await db_utils.execute(stmt)

            # Select
            stmt = sa.select(IntegrationModel.name, IntegrationModel.enabled).where(
                IntegrationModel.name == "test_async"
            )
            results = await db_utils.fetchall(stmt, as_dict=True)
            assert len(results) == 1
            assert results[0]["name"] == "test_async"

            # Cleanup
            stmt = sa.delete(IntegrationModel).where(IntegrationModel.name == "test_async")
            await db_utils.execute(stmt)

    def test_db_schema_support(self, postgres_container):
        """Test PostgreSQL db_schema parameter."""
        from ddcDatabases import PostgreSQL

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            db_schema="public",
        ) as session:
            result = session.execute(sa.text("SELECT current_schema()"))
            schema = result.scalar()
            assert schema == "public"

    def test_fetchvalue(self, postgres_container):
        """Test fetchvalue utility method."""
        from ddcDatabases import DBUtils, PostgreSQL

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        ) as session:
            Base.metadata.create_all(session.bind)
            db_utils = DBUtils(session)

            # Insert a record
            stmt = sa.insert(IntegrationModel).values(name="fetchvalue_test", enabled=True)
            db_utils.execute(stmt)

            # Fetchvalue
            stmt = sa.select(IntegrationModel.name).where(IntegrationModel.name == "fetchvalue_test")
            result = db_utils.fetchvalue(stmt)
            assert result == "fetchvalue_test"

            # Cleanup
            stmt = sa.delete(IntegrationModel).where(IntegrationModel.name == "fetchvalue_test")
            db_utils.execute(stmt)
