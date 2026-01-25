import pytest
import sqlalchemy as sa
from tests.integration.conftest import Base, IntegrationModel

pytestmark = pytest.mark.integration


class TestMySQLIntegration:
    """Integration tests for MySQL using Testcontainers."""

    def test_sync_connection(self, mysql_container):
        """Test synchronous MySQL connection and CRUD operations."""
        from ddcDatabases import DBUtils, MySQL

        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        with MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
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
    async def test_async_connection(self, mysql_container):
        """Test asynchronous MySQL connection and CRUD operations."""
        from ddcDatabases import DBUtilsAsync, MySQL

        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        # First create tables using sync connection
        with MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
        ) as session:
            Base.metadata.create_all(session.bind)

        # Now test async
        async with MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
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

    def test_pool_size_configuration(self, mysql_container):
        """Test MySQL pool size configuration works with real connection."""
        from ddcDatabases import MySQL

        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        from ddcDatabases.mysql import MySQLPoolConfig

        with MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            pool_config=MySQLPoolConfig(pool_size=5, max_overflow=10),
        ) as session:
            result = session.execute(sa.text("SELECT 1"))
            assert result.scalar() == 1

    def test_fetchvalue(self, mysql_container):
        """Test fetchvalue utility method."""
        from ddcDatabases import DBUtils, MySQL

        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        with MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
        ) as session:
            Base.metadata.create_all(session.bind)
            db_utils = DBUtils(session)

            # Insert
            stmt = sa.insert(IntegrationModel).values(name="fetchvalue_test", enabled=True)
            db_utils.execute(stmt)

            # Fetchvalue
            stmt = sa.select(IntegrationModel.name).where(IntegrationModel.name == "fetchvalue_test")
            result = db_utils.fetchvalue(stmt)
            assert result == "fetchvalue_test"

            # Cleanup
            stmt = sa.delete(IntegrationModel).where(IntegrationModel.name == "fetchvalue_test")
            db_utils.execute(stmt)
