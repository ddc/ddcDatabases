# -*- coding: utf-8 -*-
from unittest.mock import patch
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import declarative_base


Base = declarative_base()

class CoverageTestModel(Base):
    __tablename__ = 'coverage_test_model'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    enabled = Column(Boolean, default=True)


class TestSettingsEdgeCases:
    """Test settings edge cases for better coverage"""
    
    @patch('ddcDatabases.settings.load_dotenv')
    def test_dotenv_loading_flag(self, mock_load_dotenv):
        """Test dotenv loading flag behavior"""
        from ddcDatabases.settings import get_sqlite_settings

        # Clear cache and reset flag
        get_sqlite_settings.cache_clear()
        
        # Mock the flag to False
        with patch('ddcDatabases.settings._dotenv_loaded', False):
            get_sqlite_settings()
            mock_load_dotenv.assert_called_once()
            
    def test_settings_field_defaults(self):
        """Test settings default field values"""
        from ddcDatabases.settings import (
            SQLiteSettings, PostgreSQLSettings, MSSQLSettings,
            MySQLSettings, MongoDBSettings, OracleSettings
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
        assert mssql.pool_size == 20
        assert mssql.max_overflow == 10
        
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
        from ddcDatabases.exceptions import (
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
            DBUtils,
            DBUtilsAsync, 
            MSSQL,
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
