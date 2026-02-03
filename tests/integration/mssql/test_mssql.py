import pytest
import sqlalchemy as sa
from tests.integration.conftest import Base, IntegrationModel

pytestmark = pytest.mark.integration


class TestMSSQLIntegration:
    """Integration tests for MSSQL using Testcontainers."""

    def test_sync_connection(self, mssql_container):
        """Test synchronous MSSQL connection and CRUD operations."""
        from ddcDatabases import MSSQL, DBUtils

        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        with MSSQL(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="tempdb",
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
    async def test_async_connection(self, mssql_container):
        """Test asynchronous MSSQL connection and CRUD operations."""
        from ddcDatabases import MSSQL, DBUtilsAsync

        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        # First create tables using sync connection
        with MSSQL(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="tempdb",
        ) as session:
            Base.metadata.create_all(session.bind)

        # Now test async
        async with MSSQL(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="tempdb",
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

    def test_ssl_encrypt_connection(self, mssql_container):
        """Test MSSQL with SSL encrypt enabled."""
        from ddcDatabases import MSSQL

        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        from ddcDatabases.mssql import MSSQLSSLConfig

        with MSSQL(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="tempdb",
            ssl_config=MSSQLSSLConfig(ssl_encrypt=True, ssl_trust_server_certificate=True),
        ) as session:
            result = session.execute(sa.text("SELECT 1"))
            assert result.scalar() == 1
