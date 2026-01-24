import os
import pytest
import sqlalchemy as sa
import tempfile
from tests.integration.conftest import Base, IntegrationModel

pytestmark = pytest.mark.integration


class TestSqliteIntegration:
    """Integration tests for SQLite."""

    def test_sync_memory_connection(self):
        """Test synchronous SQLite in-memory connection and CRUD operations."""
        from ddcDatabases import DBUtils, Sqlite
        from sqlalchemy.pool import StaticPool

        with Sqlite(filepath=":memory:", extra_engine_args={"poolclass": StaticPool}) as session:
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

    def test_sync_file_connection(self):
        """Test synchronous SQLite file-based connection."""
        from ddcDatabases import DBUtils, Sqlite

        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            with Sqlite(filepath=db_path) as session:
                Base.metadata.create_all(session.bind)
                db_utils = DBUtils(session)

                # Insert
                stmt = sa.insert(IntegrationModel).values(name="file_test", enabled=True)
                db_utils.execute(stmt)

                # Verify
                stmt = sa.select(IntegrationModel.name).where(IntegrationModel.name == "file_test")
                result = db_utils.fetchvalue(stmt)
                assert result == "file_test"

            # Verify file was created
            assert os.path.exists(db_path)
        finally:
            os.unlink(db_path)

    def test_fetchvalue(self):
        """Test fetchvalue utility method with SQLite."""
        from ddcDatabases import DBUtils, Sqlite
        from sqlalchemy.pool import StaticPool

        with Sqlite(filepath=":memory:", extra_engine_args={"poolclass": StaticPool}) as session:
            Base.metadata.create_all(session.bind)
            db_utils = DBUtils(session)

            # Insert
            stmt = sa.insert(IntegrationModel).values(name="fetchvalue_test", enabled=True)
            db_utils.execute(stmt)

            # Fetchvalue
            stmt = sa.select(IntegrationModel.name).where(IntegrationModel.name == "fetchvalue_test")
            result = db_utils.fetchvalue(stmt)
            assert result == "fetchvalue_test"

    def test_multiple_inserts(self):
        """Test multiple inserts and fetchall with SQLite."""
        from ddcDatabases import DBUtils, Sqlite
        from sqlalchemy.pool import StaticPool

        with Sqlite(filepath=":memory:", extra_engine_args={"poolclass": StaticPool}) as session:
            Base.metadata.create_all(session.bind)
            db_utils = DBUtils(session)

            # Insert multiple records
            for i in range(5):
                stmt = sa.insert(IntegrationModel).values(name=f"item_{i}", enabled=i % 2 == 0)
                db_utils.execute(stmt)

            # Fetchall
            stmt = sa.select(IntegrationModel.name, IntegrationModel.enabled)
            results = db_utils.fetchall(stmt, as_dict=True)
            assert len(results) == 5

            # Filter enabled
            stmt = sa.select(IntegrationModel.name).where(IntegrationModel.enabled == True)
            results = db_utils.fetchall(stmt, as_dict=True)
            assert len(results) == 3  # items 0, 2, 4
