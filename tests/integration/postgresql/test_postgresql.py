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

    def test_schema_support(self, postgres_container):
        """Test PostgreSQL schema parameter."""
        from ddcDatabases import PostgreSQL

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            schema="public",
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

    def test_multi_schema_search_path(self, postgres_container):
        """Test PostgreSQL with comma-separated schemas in search_path."""
        from ddcDatabases import PostgreSQL

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        # First, create a custom schema using default connection
        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        ) as session:
            session.execute(sa.text("CREATE SCHEMA IF NOT EXISTS test_schema"))
            session.commit()

        # Connect with multi-schema search_path
        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            schema="test_schema,public",
        ) as session:
            result = session.execute(sa.text("SHOW search_path"))
            search_path = result.scalar()
            # PostgreSQL normalizes the search_path string
            assert "test_schema" in search_path
            assert "public" in search_path

        # Cleanup
        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        ) as session:
            session.execute(sa.text("DROP SCHEMA IF EXISTS test_schema CASCADE"))
            session.commit()

    @pytest.mark.asyncio
    async def test_multi_schema_search_path_async(self, postgres_container):
        """Test async PostgreSQL with comma-separated schemas in search_path."""
        from ddcDatabases import PostgreSQL

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        # Create custom schema with sync connection
        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        ) as session:
            session.execute(sa.text("CREATE SCHEMA IF NOT EXISTS test_schema_async"))
            session.commit()

        # Connect async with multi-schema search_path
        async with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            schema="test_schema_async,public",
        ) as session:
            result = await session.execute(sa.text("SHOW search_path"))
            search_path = result.scalar()
            assert "test_schema_async" in search_path
            assert "public" in search_path

        # Cleanup
        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
        ) as session:
            session.execute(sa.text("DROP SCHEMA IF EXISTS test_schema_async CASCADE"))
            session.commit()
