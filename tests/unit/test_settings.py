# -*- coding: utf-8 -*-
import os
import pytest
from unittest.mock import patch, MagicMock
from ddcDatabases.settings import (
    get_sqlite_settings,
    get_postgresql_settings, 
    get_mssql_settings,
    get_mysql_settings,
    get_mongodb_settings,
    get_oracle_settings,
    SQLiteSettings,
    PostgreSQLSettings,
    MSSQLSettings,
    MySQLSettings,
    MongoDBSettings,
    OracleSettings,
)


class TestSettingsCache:
    """Test cached settings functionality"""
    
    def test_sqlite_settings_cache(self):
        """Test SQLite settings are cached"""
        # Clear any existing cache
        get_sqlite_settings.cache_clear()
        
        settings1 = get_sqlite_settings()
        settings2 = get_sqlite_settings()
        
        # Should return same instance
        assert settings1 is settings2
        assert isinstance(settings1, SQLiteSettings)
        
    def test_postgresql_settings_cache(self):
        """Test PostgreSQL settings are cached"""
        get_postgresql_settings.cache_clear()
        
        settings1 = get_postgresql_settings()
        settings2 = get_postgresql_settings()
        
        assert settings1 is settings2
        assert isinstance(settings1, PostgreSQLSettings)
        
    def test_mssql_settings_cache(self):
        """Test MSSQL settings are cached"""
        get_mssql_settings.cache_clear()
        
        settings1 = get_mssql_settings()
        settings2 = get_mssql_settings()
        
        assert settings1 is settings2
        assert isinstance(settings1, MSSQLSettings)
        
    def test_mysql_settings_cache(self):
        """Test MySQL settings are cached"""
        get_mysql_settings.cache_clear()
        
        settings1 = get_mysql_settings()
        settings2 = get_mysql_settings()
        
        assert settings1 is settings2
        assert isinstance(settings1, MySQLSettings)
        
    def test_mongodb_settings_cache(self):
        """Test MongoDB settings are cached"""
        get_mongodb_settings.cache_clear()
        
        settings1 = get_mongodb_settings()
        settings2 = get_mongodb_settings()
        
        assert settings1 is settings2
        assert isinstance(settings1, MongoDBSettings)
        
    def test_oracle_settings_cache(self):
        """Test Oracle settings are cached"""
        get_oracle_settings.cache_clear()
        
        settings1 = get_oracle_settings()
        settings2 = get_oracle_settings()
        
        assert settings1 is settings2
        assert isinstance(settings1, OracleSettings)


class TestSQLiteSettings:
    """Test SQLite settings"""
    
    def test_default_values(self):
        """Test default SQLite settings values"""
        settings = SQLiteSettings()
        
        assert settings.file_path == "sqlite.db"
        assert settings.echo == False
        
    def test_env_override(self):
        """Test environment variable overrides"""
        with patch.dict(os.environ, {
            'SQLITE_FILE_PATH': 'tests/data/test.db',
            'SQLITE_ECHO': 'true'
        }):
            settings = SQLiteSettings()
            assert settings.file_path == 'tests/data/test.db'
            assert settings.echo == True


class TestPostgreSQLSettings:
    """Test PostgreSQL settings"""
    
    def test_default_values(self):
        """Test default PostgreSQL settings values"""
        settings = PostgreSQLSettings()
        
        assert settings.host == "localhost"
        assert settings.port == 5432
        assert settings.user == "postgres"
        assert settings.password == "postgres"
        assert settings.database == "postgres"
        assert settings.echo == False
        assert settings.async_driver == "postgresql+asyncpg"
        assert settings.sync_driver == "postgresql+psycopg2"
        
    def test_env_override(self):
        """Test environment variable overrides"""
        with patch.dict(os.environ, {
            'POSTGRESQL_HOST': 'custom-host',
            'POSTGRESQL_PORT': '9999',
            'POSTGRESQL_USER': 'testuser',
            'POSTGRESQL_PASSWORD': 'testpass',
            'POSTGRESQL_DATABASE': 'testdb',
            'POSTGRESQL_ECHO': 'true'
        }):
            settings = PostgreSQLSettings()
            assert settings.host == 'custom-host'
            assert settings.port == 9999
            assert settings.user == 'testuser'
            assert settings.password == 'testpass'
            assert settings.database == 'testdb'
            assert settings.echo == True


class TestMSSQLSettings:
    """Test MSSQL settings"""
    
    def test_default_values(self):
        """Test default MSSQL settings values"""
        settings = MSSQLSettings()
        
        assert settings.host == "localhost"
        assert settings.port == 1433
        assert settings.user == "sa"
        assert settings.password is None
        assert settings.db_schema == "dbo"
        assert settings.database == "master"
        assert settings.echo == False
        assert settings.pool_size == 20
        assert settings.max_overflow == 10
        assert settings.odbcdriver_version == 18
        assert settings.async_driver == "mssql+aioodbc"
        assert settings.sync_driver == "mssql+pyodbc"


class TestMySQLSettings:
    """Test MySQL settings"""
    
    def test_default_values(self):
        """Test default MySQL settings values"""
        settings = MySQLSettings()
        
        assert settings.host == "localhost"
        assert settings.port == 3306
        assert settings.user == "root"
        assert settings.password == "root"
        assert settings.database == "dev"
        assert settings.echo == False
        assert settings.async_driver == "mysql+aiomysql"
        assert settings.sync_driver == "mysql+pymysql"


class TestMongoDBSettings:
    """Test MongoDB settings"""
    
    def test_default_values(self):
        """Test default MongoDB settings values"""
        settings = MongoDBSettings()
        
        assert settings.host == "localhost"
        assert settings.port == 27017
        assert settings.user == "admin"
        assert settings.password == "admin"
        assert settings.database == "admin"
        assert settings.batch_size == 2865
        assert settings.limit == 0
        assert settings.sync_driver == "mongodb"


class TestOracleSettings:
    """Test Oracle settings"""
    
    def test_default_values(self):
        """Test default Oracle settings values"""
        settings = OracleSettings()
        
        assert settings.host == "localhost"
        assert settings.port == 1521
        assert settings.user == "system"
        assert settings.password == "oracle"
        assert settings.servicename == "xe"
        assert settings.echo == False
        assert settings.sync_driver == "oracle+cx_oracle"


class TestDotenvLoading:
    """Test dotenv loading functionality"""
    
    @patch('ddcDatabases.settings._dotenv_loaded', False)
    @patch('ddcDatabases.settings.load_dotenv')
    def test_dotenv_loaded_once(self, mock_load_dotenv):
        """Test that dotenv is only loaded once"""
        get_sqlite_settings.cache_clear()
        
        # First call should load dotenv
        get_sqlite_settings()
        assert mock_load_dotenv.call_count == 1
        
        # Second call should not load dotenv again
        get_sqlite_settings()
        assert mock_load_dotenv.call_count == 1
        
    @patch('ddcDatabases.settings._dotenv_loaded', True)
    @patch('ddcDatabases.settings.load_dotenv')
    def test_dotenv_not_loaded_if_already_loaded(self, mock_load_dotenv):
        """Test that dotenv is not loaded if already loaded"""
        get_postgresql_settings.cache_clear()
        
        get_postgresql_settings()
        mock_load_dotenv.assert_not_called()
