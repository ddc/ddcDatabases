import tempfile
from unittest.mock import MagicMock, patch
import pytest
import sqlalchemy as sa
from tests.models.test_models import ModelTest


class TestSQLite:
    """Test SQLite database connection class"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases import Sqlite, DBUtils

        self.Sqlite = Sqlite
        self.DBUtils = DBUtils

    @patch('ddcDatabases.sqlite.get_sqlite_settings')
    def test_init_basic(self, mock_get_settings):
        """Test SQLite basic initialization"""
        mock_settings = MagicMock()
        mock_settings.file_path = "sqlite.db"
        mock_settings.echo = False
        mock_get_settings.return_value = mock_settings

        sqlite = self.Sqlite()

        assert sqlite.filepath == "sqlite.db"
        assert sqlite.echo == False
        assert sqlite.is_connected == False

    @patch('ddcDatabases.sqlite.get_sqlite_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test SQLite initialization with parameters"""
        mock_settings = MagicMock()
        mock_settings.file_path = "default.db"
        mock_settings.echo = False
        mock_get_settings.return_value = mock_settings

        sqlite = self.Sqlite(filepath="custom.db", echo=True, autoflush=False, expire_on_commit=False)

        assert sqlite.filepath == "custom.db"
        assert sqlite.echo == True
        assert sqlite.autoflush == False
        assert sqlite.expire_on_commit == False

    def test_real_operations(self):
        """Test comprehensive SQLite operations"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        with self.Sqlite(filepath=db_path) as session:
            # Create table
            ModelTest.__table__.create(session.bind, checkfirst=True)

            db_utils = self.DBUtils(session)

            # Test insert single
            test_obj = ModelTest(id=1, name="test1", enabled=True)
            db_utils.insert(test_obj)

            # Test fetchall with correct ORM access pattern
            stmt = sa.select(ModelTest)
            results = db_utils.fetchall(stmt)
            assert len(results) == 1
            # Access through the model object returned by ORM select
            assert results[0]['ModelTest'].name == "test1"

            # Test fetchvalue
            stmt = sa.select(ModelTest.name).where(ModelTest.id == 1)
            name = db_utils.fetchvalue(stmt)
            assert name == "test1"

            # Test insertbulk
            bulk_data = [
                {"id": 2, "name": "test2", "enabled": False},
                {"id": 3, "name": "test3", "enabled": True},
                {"id": 4, "name": "test4", "enabled": False},
            ]
            db_utils.insertbulk(ModelTest, bulk_data)

            # Verify bulk insert worked
            stmt = sa.select(ModelTest)
            results = db_utils.fetchall(stmt)
            assert len(results) == 4

            # Test execute (update)
            stmt = sa.update(ModelTest).where(ModelTest.id == 1).values(name="updated_test1")
            db_utils.execute(stmt)

            # Verify update
            stmt = sa.select(ModelTest.name).where(ModelTest.id == 1)
            updated_name = db_utils.fetchvalue(stmt)
            assert updated_name == "updated_test1"

            # Test deleteall
            db_utils.deleteall(ModelTest)

            # Verify deletion
            stmt = sa.select(ModelTest)
            results = db_utils.fetchall(stmt)
            assert len(results) == 0

    def test_fetchvalue_none_case(self):
        """Test fetchvalue returning None"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        with self.Sqlite(filepath=db_path) as session:
            ModelTest.__table__.create(session.bind, checkfirst=True)
            db_utils = self.DBUtils(session)

            # Query non-existent record
            stmt = sa.select(ModelTest.name).where(ModelTest.id == 999)
            result = db_utils.fetchvalue(stmt)
            assert result is None

    def test_context_manager(self):
        """Test SQLite context manager entry/exit"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        sqlite = self.Sqlite(filepath=db_path)

        # Test __enter__
        session = sqlite.__enter__()
        assert session is not None
        assert sqlite.is_connected == True
        assert sqlite.session is session

        # Test __exit__
        sqlite.__exit__(None, None, None)
        assert sqlite.is_connected == False

    @patch('ddcDatabases.sqlite.create_engine')
    def test_engine_creation_error(self, mock_create_engine):
        """Test SQLite engine creation error handling"""
        mock_create_engine.side_effect = Exception("Engine creation failed")

        sqlite = self.Sqlite(filepath="test.db")

        with pytest.raises(Exception, match="Engine creation failed"):
            with sqlite._get_engine():
                pass

    def test_optional_parameters(self):
        """Test SQLite with all optional parameters"""
        extra_args = {"pool_timeout": 60, "connect_timeout": 30}

        sqlite = self.Sqlite(
            filepath="custom.db", echo=True, autoflush=False, expire_on_commit=False, extra_engine_args=extra_args
        )

        assert sqlite.filepath == "custom.db"
        assert sqlite.echo == True
        assert sqlite.autoflush == False
        assert sqlite.expire_on_commit == False
        assert sqlite.extra_engine_args == extra_args


class TestSQLiteRealOperations:
    """Test with real SQLite database operations"""

    def setup_method(self):
        """Import dependencies when needed"""
        from ddcDatabases import Sqlite, DBUtils

        self.Sqlite = Sqlite
        self.DBUtils = DBUtils

    def test_real_fetchall(self):
        """Test fetchall with real database"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        with self.Sqlite(filepath=db_path) as session:
            ModelTest.__table__.create(session.bind, checkfirst=True)

            # Insert test data
            test_obj = ModelTest(id=1, name="test", enabled=True)
            session.add(test_obj)
            session.commit()

            db_utils = self.DBUtils(session)
            stmt = sa.select(ModelTest).where(ModelTest.id == 1)
            results = db_utils.fetchall(stmt)

            assert len(results) == 1
            # Access through the model object returned by ORM select
            assert results[0]['ModelTest'].id == 1
            assert results[0]['ModelTest'].name == "test"
            assert results[0]['ModelTest'].enabled == True

    def test_real_fetchvalue(self):
        """Test fetchvalue with real database"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        with self.Sqlite(filepath=db_path) as session:
            ModelTest.__table__.create(session.bind, checkfirst=True)

            # Insert test data
            test_obj = ModelTest(id=1, name="test_value", enabled=True)
            session.add(test_obj)
            session.commit()

            db_utils = self.DBUtils(session)
            stmt = sa.select(ModelTest.name).where(ModelTest.id == 1)
            result = db_utils.fetchvalue(stmt)

            assert result == "test_value"

    def test_real_insertbulk(self):
        """Test bulk insert with real database"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        with self.Sqlite(filepath=db_path) as session:
            ModelTest.__table__.create(session.bind, checkfirst=True)

            db_utils = self.DBUtils(session)
            bulk_data = [
                {"id": 1, "name": "test1", "enabled": True},
                {"id": 2, "name": "test2", "enabled": False},
                {"id": 3, "name": "test3", "enabled": True},
            ]

            db_utils.insertbulk(ModelTest, bulk_data)

            # Verify data was inserted
            stmt = sa.select(ModelTest)
            results = db_utils.fetchall(stmt)

            assert len(results) == 3
            # Access through the model object returned by ORM select
            assert results[0]['ModelTest'].name == "test1"
            assert results[1]['ModelTest'].name == "test2"
            assert results[2]['ModelTest'].name == "test3"

    def test_connection_state_management(self):
        """Test connection state management"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        sqlite = self.Sqlite(filepath=db_path)

        # Initially not connected
        assert sqlite.is_connected == False

        # Test context manager connection
        with sqlite as session:
            assert sqlite.is_connected == True
            assert session is not None

        # After context manager, should be disconnected
        assert sqlite.is_connected == False

    def test_custom_settings_integration(self):
        """Test integration with custom settings"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name

        # Test with custom settings
        sqlite = self.Sqlite(filepath=db_path, echo=True, autoflush=True, expire_on_commit=True)

        assert sqlite.filepath == db_path
        assert sqlite.echo == True
        assert sqlite.autoflush == True
        assert sqlite.expire_on_commit == True

        # Test that it works with real database
        with sqlite as session:
            ModelTest.__table__.create(session.bind, checkfirst=True)

            # Insert and verify
            test_obj = ModelTest(id=1, name="settings_test", enabled=True)
            session.add(test_obj)
            session.commit()

            db_utils = self.DBUtils(session)
            stmt = sa.select(ModelTest.name).where(ModelTest.id == 1)
            result = db_utils.fetchvalue(stmt)
            assert result == "settings_test"
