# -*- coding: utf-8 -*-
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from ddcDatabases.mssql import MSSQL


class TestMSSQL:
    """Test MSSQL database connection class"""
    
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_init_basic(self, mock_get_settings):
        """Test MSSQL basic initialization"""
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.database = "master"
        mock_settings.db_schema = "dbo"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 17
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_get_settings.return_value = mock_settings
        
        mssql = MSSQL()
        
        assert mssql.connection_url["host"] == "localhost"
        assert mssql.connection_url["port"] == 1433
        assert mssql.connection_url["database"] == "master"
        assert mssql.connection_url["username"] == "sa"
        assert mssql.connection_url["password"] == "password"
        assert mssql.schema == "dbo"
        assert mssql.sync_driver == "mssql+pyodbc"
        assert mssql.async_driver == "mssql+aioodbc"
        
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test MSSQL initialization with override parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "defaultuser"
        mock_settings.password = "defaultpass"
        mock_settings.host = "defaulthost"
        mock_settings.port = 1433
        mock_settings.database = "defaultdb"
        mock_settings.db_schema = "dbo"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 17
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_get_settings.return_value = mock_settings
        
        mssql = MSSQL(
            host="customhost",
            port=1434,
            user="customuser",
            password="custompass",
            database="customdb",
            schema="customschema",
            echo=True,
            pool_size=30,
            max_overflow=20
        )
        
        assert mssql.connection_url["host"] == "customhost"
        assert mssql.connection_url["port"] == 1434
        assert mssql.connection_url["database"] == "customdb"
        assert mssql.connection_url["username"] == "customuser"
        assert mssql.connection_url["password"] == "custompass"
        assert mssql.schema == "customschema"
        assert mssql.echo == True
        assert mssql.pool_size == 30
        assert mssql.max_overflow == 20
        
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_missing_credentials_error(self, mock_get_settings):
        """Test RuntimeError when credentials are missing - Line 32"""
        mock_settings = MagicMock()
        mock_settings.user = None  # Missing user
        mock_settings.password = "password"
        mock_get_settings.return_value = mock_settings
        
        with pytest.raises(RuntimeError, match="Missing username/password"):
            MSSQL()
            
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_missing_password_error(self, mock_get_settings):
        """Test RuntimeError when password is missing - Line 32"""
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = None  # Missing password
        mock_get_settings.return_value = mock_settings
        
        with pytest.raises(RuntimeError, match="Missing username/password"):
            MSSQL()
    
    @patch('ddcDatabases.mssql.get_mssql_settings')
    @patch('ddcDatabases.mssql.ConnectionTester')
    def test_test_connection_sync(self, mock_test_connections, mock_get_settings):
        """Test _test_connection_sync method - Lines 72-83"""
        # Setup mock settings
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.database = "master"
        mock_settings.db_schema = "dbo"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 17
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_get_settings.return_value = mock_settings
        
        # Setup mock test connection
        mock_test_conn_instance = MagicMock()
        mock_test_connections.return_value = mock_test_conn_instance
        
        mssql = MSSQL()
        mock_session = MagicMock(spec=Session)
        
        # Call the method under test
        mssql._test_connection_sync(mock_session)
        
        # Verify ConnectionTester was created with correct parameters
        mock_test_connections.assert_called_once()
        call_args = mock_test_connections.call_args
        assert call_args[1]['sync_session'] is mock_session
        assert isinstance(call_args[1]['host_url'], URL)
        
        # Verify test_connection_sync was called
        mock_test_conn_instance.test_connection_sync.assert_called_once()
        
    @patch('ddcDatabases.mssql.get_mssql_settings')
    @patch('ddcDatabases.mssql.ConnectionTester')
    @pytest.mark.asyncio
    async def test_test_connection_async(self, mock_test_connections, mock_get_settings):
        """Test _test_connection_async method - Lines 86-97"""
        # Setup mock settings
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.database = "master"
        mock_settings.db_schema = "dbo"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 17
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_get_settings.return_value = mock_settings
        
        # Setup mock test connection
        mock_test_conn_instance = MagicMock()
        mock_test_conn_instance.test_connection_async = AsyncMock()
        mock_test_connections.return_value = mock_test_conn_instance
        
        mssql = MSSQL()
        mock_session = AsyncMock(spec=AsyncSession)
        
        # Call the method under test
        await mssql._test_connection_async(mock_session)
        
        # Verify ConnectionTester was created with correct parameters
        mock_test_connections.assert_called_once()
        call_args = mock_test_connections.call_args
        assert call_args[1]['async_session'] is mock_session
        assert isinstance(call_args[1]['host_url'], URL)
        
        # Verify test_connection_async was called
        mock_test_conn_instance.test_connection_async.assert_called_once()
    
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_connection_query_string(self, mock_get_settings):
        """Test MSSQL connection query string formation"""
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.database = "master"
        mock_settings.db_schema = "dbo"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 17
        mock_settings.async_driver = "mssql+aioodbc"
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_get_settings.return_value = mock_settings
        
        mssql = MSSQL()
        
        # Test that query dict is properly formed
        expected_driver = "ODBC Driver 17 for SQL Server"
        assert "query" in mssql.connection_url
        assert "driver" in mssql.connection_url["query"]
        assert mssql.connection_url["query"]["driver"] == expected_driver
        assert "TrustServerCertificate" in mssql.connection_url["query"]
        
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_url_creation_with_schema(self, mock_get_settings):
        """Test URL creation includes schema in query parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.host = "testhost"
        mock_settings.port = 1433
        mock_settings.database = "testdb"
        mock_settings.db_schema = "custom_schema"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 17
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_get_settings.return_value = mock_settings
        
        mssql = MSSQL()
        
        # Test that schema is properly set
        assert mssql.schema == "custom_schema"
        
        # Test connection URL structure
        assert mssql.connection_url["host"] == "testhost"
        assert mssql.connection_url["database"] == "testdb"
        assert mssql.connection_url["query"]["driver"] == "ODBC Driver 17 for SQL Server"
        assert mssql.connection_url["query"]["TrustServerCertificate"] == "yes"
    
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_custom_odbcdriver_version(self, mock_get_settings):
        """Test custom ODBC driver version"""
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.database = "master"
        mock_settings.db_schema = "dbo"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 18  # Different version
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_get_settings.return_value = mock_settings
        
        mssql = MSSQL()
        
        # Test that ODBC driver version is properly set
        assert mssql.odbcdriver_version == 18
        assert mssql.connection_url["query"]["driver"] == "ODBC Driver 18 for SQL Server"
    
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_extra_engine_args(self, mock_get_settings):
        """Test extra engine arguments are properly included"""
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.database = "master"
        mock_settings.db_schema = "dbo"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 17
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_get_settings.return_value = mock_settings
        
        extra_args = {"custom_arg": "custom_value", "timeout": 30}
        mssql = MSSQL(extra_engine_args=extra_args)
        
        # Test that extra args are included in engine_args
        assert "custom_arg" in mssql.engine_args
        assert mssql.engine_args["custom_arg"] == "custom_value"
        assert mssql.engine_args["timeout"] == 30
        
        # Test that default args are still present
        assert mssql.engine_args["pool_size"] == 20
        assert mssql.engine_args["max_overflow"] == 10
        assert mssql.engine_args["echo"] == False
        
    @patch('ddcDatabases.mssql.get_mssql_settings')
    def test_autoflush_and_expire_on_commit(self, mock_get_settings):
        """Test MSSQL autoflush and expire_on_commit parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.database = "master"
        mock_settings.db_schema = "dbo"
        mock_settings.echo = False
        mock_settings.pool_size = 20
        mock_settings.max_overflow = 10
        mock_settings.odbcdriver_version = 17
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_get_settings.return_value = mock_settings
        
        mssql = MSSQL(autoflush=False, expire_on_commit=False)
        
        assert mssql.autoflush == False
        assert mssql.expire_on_commit == False
