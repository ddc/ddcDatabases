import pytest
import sqlalchemy as sa
from tests.integration.conftest import Base, IntegrationModel

pytestmark = pytest.mark.integration


class TestOracleIntegration:
    """Integration tests for Oracle using Testcontainers."""

    def test_sync_connection(self, oracle_container):
        """Test synchronous Oracle connection and CRUD operations."""
        from ddcDatabases import DBUtils, Oracle

        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        with Oracle(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
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

    def test_oracle_dual_query(self, oracle_container):
        """Test Oracle-specific SELECT FROM dual."""
        from ddcDatabases import Oracle

        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        with Oracle(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
        ) as session:
            result = session.execute(sa.text("SELECT 1 FROM dual"))
            assert result.scalar() == 1

    def test_pool_configuration(self, oracle_container):
        """Test Oracle pool configuration works with real connection."""
        from ddcDatabases import Oracle

        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        from ddcDatabases.oracle import OraclePoolConfig

        with Oracle(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
            pool_config=OraclePoolConfig(pool_size=5, max_overflow=10, pool_recycle=1800),
        ) as session:
            result = session.execute(sa.text("SELECT 1 FROM dual"))
            assert result.scalar() == 1
