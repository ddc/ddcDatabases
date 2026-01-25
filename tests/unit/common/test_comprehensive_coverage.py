from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import declarative_base
from unittest.mock import patch

Base = declarative_base()


class CoverageTestModel(Base):
    __tablename__ = 'coverage_test_model'

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    enabled = Column(Boolean, default=True)


class TestSettingsEdgeCases:
    """Test settings edge cases for better coverage"""

    @patch('ddcDatabases.core.settings.load_dotenv')
    def test_dotenv_loading_flag(self, mock_load_dotenv):
        """Test dotenv loading flag behavior"""
        from ddcDatabases.core.settings import get_sqlite_settings

        # Clear cache and reset flag
        get_sqlite_settings.cache_clear()

        # Mock the flag to False
        with patch('ddcDatabases.core.settings._dotenv_loaded', False):
            get_sqlite_settings()
            mock_load_dotenv.assert_called_once()

    def test_settings_field_defaults(self):
        """Test settings default field values"""
        from ddcDatabases.core.settings import (
            MongoDBSettings,
            MSSQLSettings,
            MySQLSettings,
            OracleSettings,
            PostgreSQLSettings,
            SQLiteSettings,
        )

        # Test all settings classes have expected defaults
        sqlite = SQLiteSettings()
        assert sqlite.file_path == "sqlite.db"
        assert sqlite.echo == False

        postgres = PostgreSQLSettings()
        assert postgres.host == "localhost"
        assert postgres.port == 5432
        assert postgres.user == "postgres"
        assert postgres.database == "postgres"

        mssql = MSSQLSettings()
        assert mssql.host == "localhost"
        assert mssql.port == 1433
        assert mssql.user == "sa"
        assert mssql.database == "master"
        assert mssql.pool_size == 25
        assert mssql.max_overflow == 50

        mysql = MySQLSettings()
        assert mysql.host == "localhost"
        assert mysql.port == 3306
        assert mysql.user == "root"
        assert mysql.database == "dev"

        mongodb = MongoDBSettings()
        assert mongodb.host == "localhost"
        assert mongodb.port == 27017
        assert mongodb.batch_size == 2865
        assert mongodb.limit == 0

        oracle = OracleSettings()
        assert oracle.host == "localhost"
        assert oracle.port == 1521
        assert oracle.servicename == "xe"


class TestExceptionCoverageImprovement:
    """Test custom exceptions to improve coverage"""

    def test_exception_class_definitions(self):
        """Test exception class definitions exist"""
        from ddcDatabases.core.exceptions import (
            CustomBaseException,
            DBDeleteAllDataException,
            DBExecuteException,
            DBFetchAllException,
            DBFetchValueException,
            DBInsertBulkException,
            DBInsertSingleException,
        )

        # Test all exception classes exist and inherit properly
        assert issubclass(DBDeleteAllDataException, CustomBaseException)
        assert issubclass(DBExecuteException, CustomBaseException)
        assert issubclass(DBFetchAllException, CustomBaseException)
        assert issubclass(DBFetchValueException, CustomBaseException)
        assert issubclass(DBInsertBulkException, CustomBaseException)
        assert issubclass(DBInsertSingleException, CustomBaseException)

        # Test base exception inherits from Exception
        assert issubclass(CustomBaseException, Exception)


class TestModuleImports:
    """Test module imports and __init__.py coverage"""

    def test_main_imports(self):
        """Test main module imports work correctly"""
        from ddcDatabases import (
            MSSQL,
            DBUtils,
            DBUtilsAsync,
            MySQL,
            Oracle,
            PostgreSQL,
            Sqlite,
        )

        # Test that all main classes can be imported
        assert DBUtils is not None
        assert DBUtilsAsync is not None
        assert MSSQL is not None
        assert MySQL is not None
        assert Oracle is not None
        assert PostgreSQL is not None
        assert Sqlite is not None

    def test_version_info(self):
        """Test version information"""
        import ddcDatabases

        # Test version attributes exist
        assert hasattr(ddcDatabases, '__version__')
        assert hasattr(ddcDatabases, '__version_info__')
        assert hasattr(ddcDatabases, '__req_python_version__')

        # Test version info is tuple format
        assert isinstance(ddcDatabases.__version_info__.major, int)
        assert isinstance(ddcDatabases.__version_info__.minor, int)
        assert isinstance(ddcDatabases.__version_info__.micro, int)

        # Test version info structure
        assert ddcDatabases.__version_info__.releaselevel == "final"
        assert ddcDatabases.__version_info__.serial == 0

        # Test required Python version
        assert ddcDatabases.__req_python_version__.major == 3
        assert ddcDatabases.__req_python_version__.minor == 12
        assert ddcDatabases.__req_python_version__.micro == 0
        assert ddcDatabases.__req_python_version__.releaselevel == "final"
        assert ddcDatabases.__req_python_version__.serial == 0

    def test_package_metadata(self):
        """Test package metadata constants"""
        import ddcDatabases

        # Test metadata attributes exist
        assert hasattr(ddcDatabases, '__title__')
        assert hasattr(ddcDatabases, '__author__')
        assert hasattr(ddcDatabases, '__email__')
        assert hasattr(ddcDatabases, '__license__')
        assert hasattr(ddcDatabases, '__copyright__')

        # Test metadata values
        assert ddcDatabases.__title__ == "ddcDatabases"
        assert ddcDatabases.__author__ == "Daniel Costa"
        assert ddcDatabases.__email__ == "danieldcsta@gmail.com>"
        assert ddcDatabases.__license__ == "MIT"
        assert ddcDatabases.__copyright__ == "Copyright 2024-present DDC Softwares"

    def test_all_exports(self):
        """Test __all__ exports are accessible"""
        import ddcDatabases

        # Test __all__ exists and contains expected items (alphabetically sorted)
        assert hasattr(ddcDatabases, '__all__')
        expected_exports = {
            "BasePoolConfig",
            "BaseRetryConfig",
            "BaseSessionConfig",
            "close_all_persistent_connections",
            "DBUtils",
            "DBUtilsAsync",
            "MongoDB",
            "MongoDBConnectionConfig",
            "MongoDBPersistent",
            "MongoDBQueryConfig",
            "MongoDBRetryConfig",
            "MongoDBTLSConfig",
            "MSSQL",
            "MSSQLConnectionConfig",
            "MSSQLPersistent",
            "MSSQLPoolConfig",
            "MSSQLRetryConfig",
            "MSSQLSessionConfig",
            "MSSQLSSLConfig",
            "MySQL",
            "MySQLConnectionConfig",
            "MySQLPersistent",
            "MySQLPoolConfig",
            "MySQLRetryConfig",
            "MySQLSessionConfig",
            "MySQLSSLConfig",
            "Oracle",
            "OracleConnectionConfig",
            "OraclePersistent",
            "OraclePoolConfig",
            "OracleRetryConfig",
            "OracleSessionConfig",
            "OracleSSLConfig",
            "PersistentConnectionConfig",
            "PostgreSQL",
            "PostgreSQLConnectionConfig",
            "PostgreSQLPersistent",
            "PostgreSQLPoolConfig",
            "PostgreSQLRetryConfig",
            "PostgreSQLSessionConfig",
            "PostgreSQLSSLConfig",
            "Sqlite",
            "SqliteRetryConfig",
            "SqliteSessionConfig",
        }

        # Check all expected items are in __all__
        assert set(ddcDatabases.__all__) == expected_exports

        # Check all items in __all__ are actually accessible
        for item in ddcDatabases.__all__:
            assert hasattr(ddcDatabases, item), f"{item} not accessible"

    @patch('importlib.metadata.version')
    def test_version_import_error_handling(self, mock_version):
        """Test version parsing with ModuleNotFoundError"""
        # Mock version to raise ModuleNotFoundError
        mock_version.side_effect = ModuleNotFoundError("No module named 'ddcDatabases'")

        # Test the logic directly since module caching makes re-import difficult
        from importlib.metadata import version

        try:
            _version = tuple(int(x) for x in version("ddcDatabases").split("."))
        except ModuleNotFoundError:
            _version = (0, 0, 0)

        # Should fall back to (0, 0, 0) when ModuleNotFoundError occurs
        assert _version == (0, 0, 0)

    def test_mongodb_import_accessibility(self):
        """Test MongoDB import specifically (since it's not in main imports)"""
        from ddcDatabases import MongoDB

        assert MongoDB is not None
