# -*- coding: utf-8 -*-
from io import StringIO
from unittest.mock import MagicMock, patch
import pytest
from ddcDatabases.mongodb import MongoDB


class TestMongoDB:
    """Test MongoDB database connection class"""
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_init_with_settings(self, mock_get_settings):
        """Test MongoDB initialization with settings"""
        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        mongodb = MongoDB()
        
        assert mongodb.host == "localhost"
        assert mongodb.port == 27017
        assert mongodb.user == "admin"
        assert mongodb.password == "admin"
        assert mongodb.database == "testdb"
        assert mongodb.batch_size == 1000
        assert mongodb.limit == 0
        assert mongodb.is_connected == False
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test MongoDB initialization with override parameters"""
        mock_settings = MagicMock()
        mock_settings.host = "defaulthost"
        mock_settings.port = 27017
        mock_settings.user = "defaultuser"
        mock_settings.password = "defaultpass"
        mock_settings.database = "defaultdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_get_settings.return_value = mock_settings
        
        mongodb = MongoDB(
            host="customhost",
            port=27018,
            user="customuser",
            password="custompass",
            database="customdb",
            batch_size=500,
            limit=100
        )
        
        assert mongodb.host == "customhost"
        assert mongodb.port == 27018
        assert mongodb.user == "customuser"
        assert mongodb.password == "custompass"
        assert mongodb.database == "customdb"
        assert mongodb.batch_size == 500
        assert mongodb.limit == 100
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_missing_credentials_error(self, mock_get_settings):
        """Test RuntimeError when credentials are missing - Line 27"""
        mock_settings = MagicMock()
        mock_settings.user = None  # Missing user
        mock_settings.password = "admin"
        mock_get_settings.return_value = mock_settings
        
        with pytest.raises(RuntimeError, match="Missing username/password"):
            MongoDB()
            
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_missing_password_error(self, mock_get_settings):
        """Test RuntimeError when password is missing - Line 27"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = None  # Missing password
        mock_get_settings.return_value = mock_settings
        
        with pytest.raises(RuntimeError, match="Missing username/password"):
            MongoDB()
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    def test_enter_context_manager(self, mock_mongo_client, mock_get_settings):
        """Test MongoDB context manager entry"""
        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        mock_client = MagicMock()
        # Mock the ping method for connection testing
        mock_client.admin.command.return_value = True
        mock_mongo_client.return_value = mock_client
        
        mongodb = MongoDB()
        
        with mongodb as mongo_instance:
            assert mongo_instance is mongodb  # Returns self, not client
            assert mongodb.is_connected == True
            assert mongodb.client is mock_client
            
        # Check that MongoDB connection was established with correct sync_driver format
        mock_mongo_client.assert_called_once()
        call_args = mock_mongo_client.call_args[0][0]
        # Connection string should be: mongodb://admin:admin@localhost/testdb
        assert call_args == "mongodb://admin:admin@localhost/testdb"
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    @patch('sys.exit')
    def test_enter_context_manager_exception_handling(self, mock_sys_exit, mock_mongo_client, mock_get_settings):
        """Test exception handling in __enter__ method - Lines 47-49"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        # Mock MongoClient to raise an exception
        mock_mongo_client.side_effect = Exception("Connection failed")
        
        mongodb = MongoDB()
        
        # Call __enter__ which should handle the exception
        mongodb.__enter__()
        
        # Verify sys.exit was called due to exception
        mock_sys_exit.assert_called_once_with(1)
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    @patch('sys.exit')
    def test_enter_client_close_on_exception(self, mock_sys_exit, mock_mongo_client, mock_get_settings):
        """Test client.close() is called when exception occurs - Lines 47-49"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        # Mock client and test_connection to simulate failure after client creation
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        mongodb = MongoDB()
        
        # Mock _test_connection to raise exception after client is created
        with patch.object(mongodb, '_test_connection', side_effect=Exception("Test connection failed")):
            mongodb.__enter__()
        
        # Verify client.close() was called before sys.exit
        mock_client.close.assert_called_once()
        mock_sys_exit.assert_called_once_with(1)
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    def test_exit_context_manager(self, mock_mongo_client, mock_get_settings):
        """Test MongoDB context manager exit"""
        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        mock_client = MagicMock()
        mock_mongo_client.return_value = mock_client
        
        mongodb = MongoDB()
        
        with mongodb:
            pass
            
        assert mongodb.is_connected == False
        mock_client.close.assert_called_once()
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    @patch('sys.stderr', new_callable=StringIO)
    def test_test_connection_exception_handling(self, mock_stderr, mock_mongo_client, mock_get_settings):
        """Test exception handling in _test_connection method - Lines 65-67"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        # Setup mock client with failing ping command
        mock_client = MagicMock()
        mock_client.admin.command.side_effect = Exception("Ping failed")
        mock_mongo_client.return_value = mock_client
        
        mongodb = MongoDB()
        mongodb.client = mock_client
        
        # Call _test_connection
        result = mongodb._test_connection()
        
        # Verify it returns False on exception
        assert result == False
        
        # Verify error message was written to stderr
        stderr_output = mock_stderr.getvalue()
        assert "[ERROR]:Connection to database failed" in stderr_output
        assert "admin@localhost/testdb" in stderr_output
        assert "Ping failed" in stderr_output
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    @patch('sys.stdout', new_callable=StringIO)
    def test_test_connection_success_logging(self, mock_stdout, mock_mongo_client, mock_get_settings):
        """Test successful connection logging in _test_connection method"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        # Setup mock client with successful ping command
        mock_client = MagicMock()
        mock_client.admin.command.return_value = True
        mock_mongo_client.return_value = mock_client
        
        mongodb = MongoDB()
        mongodb.client = mock_client
        
        # Call _test_connection
        result = mongodb._test_connection()
        
        # Verify it returns True on success
        assert result == True
        
        # Verify success message was written to stdout
        stdout_output = mock_stdout.getvalue()
        assert "[INFO]:Connection to database successful" in stdout_output
        assert "admin@localhost/testdb" in stdout_output
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    def test_cursor_context_manager(self, mock_mongo_client, mock_get_settings):
        """Test MongoDB cursor context manager"""
        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_mongo_client.return_value = mock_client
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = mock_cursor
        
        mongodb = MongoDB()
        mongodb.client = mock_client  # Set client directly for testing
        
        query = {"name": "test"}
        collection_name = "test_collection"
        
        with mongodb.cursor(collection_name, query) as cursor:
            assert cursor is mock_cursor
            
        mock_client.__getitem__.assert_called_once_with("testdb")
        mock_database.__getitem__.assert_called_once_with(collection_name)
        mock_collection.find.assert_called_once_with(query, batch_size=1000, limit=0)
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    def test_cursor_with_custom_batch_size_and_limit(self, mock_mongo_client, mock_get_settings):
        """Test MongoDB cursor with custom batch size and limit"""
        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.database = "testdb"
        mock_settings.batch_size = 2000
        mock_settings.limit = 500
        mock_get_settings.return_value = mock_settings
        
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_mongo_client.return_value = mock_client
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = mock_cursor
        
        mongodb = MongoDB(batch_size=2000, limit=500)
        mongodb.client = mock_client  # Set client directly for testing
        
        query = {"status": "active"}
        collection_name = "users"
        
        with mongodb.cursor(collection_name, query):
            pass
            
        mock_collection.find.assert_called_once_with(query, batch_size=2000, limit=500)
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_cursor_without_client(self, mock_get_settings):
        """Test cursor method when client is not initialized"""
        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        mongodb = MongoDB()
        
        # Set up a mock client for the cursor test
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = mock_cursor
        
        mongodb.client = mock_client
        
        query = {"name": "test"}
        collection_name = "test_collection"
        
        # Should work with client set
        with mongodb.cursor(collection_name, query) as cursor:
            assert cursor is mock_cursor
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_cursor_with_sorting(self, mock_get_settings):
        """Test cursor method with sorting functionality - Lines 73-74"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        # Setup mock client and collection
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = mock_cursor
        
        mongodb = MongoDB()
        mongodb.client = mock_client
        
        query = {"status": "active"}
        collection_name = "test_collection"
        sort_column = "created_at"
        sort_direction = "descending"
        
        # Test cursor with sorting - this should cover lines 73-74
        with mongodb.cursor(collection_name, query, sort_column, sort_direction) as cursor:
            assert cursor is mock_cursor
            
        # Verify create_index was called with DESCENDING sort
        from pymongo import DESCENDING
        mock_collection.create_index.assert_called_once_with([("created_at", DESCENDING)])
        
        # Verify find was called with correct parameters
        mock_collection.find.assert_called_once_with(query, batch_size=1000, limit=0)
        
        # Verify cursor methods were called
        mock_cursor.batch_size.assert_called_once_with(1000)
        mock_cursor.close.assert_called_once()
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_cursor_with_ascending_sort(self, mock_get_settings):
        """Test cursor method with ascending sort direction - Lines 73-74"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        # Setup mock client and collection
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = mock_cursor
        
        mongodb = MongoDB()
        mongodb.client = mock_client
        
        # Test with ascending sort direction
        with mongodb.cursor("test_collection", {}, "name", "ascending"):
            pass
            
        # Verify create_index was called with ASCENDING sort
        from pymongo import ASCENDING
        mock_collection.create_index.assert_called_once_with([("name", ASCENDING)])
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_cursor_without_sorting(self, mock_get_settings):
        """Test cursor method without sorting parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        # Setup mock client and collection
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = mock_cursor
        
        mongodb = MongoDB()
        mongodb.client = mock_client
        
        # Test cursor without sorting parameters
        with mongodb.cursor("test_collection"):
            pass
            
        # Verify create_index was NOT called when no sort parameters provided
        mock_collection.create_index.assert_not_called()
        
        # Verify find was called with empty query
        mock_collection.find.assert_called_once_with({}, batch_size=1000, limit=0)
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')  
    def test_various_sort_direction_formats(self, mock_get_settings):
        """Test different sort direction string formats - Line 73"""
        mock_settings = MagicMock()
        mock_settings.user = "admin"
        mock_settings.password = "admin"
        mock_settings.host = "localhost"
        mock_settings.port = 27017
        mock_settings.database = "testdb"
        mock_settings.batch_size = 1000
        mock_settings.limit = 0
        mock_settings.sync_driver = "mongodb"
        mock_get_settings.return_value = mock_settings
        
        # Setup mock client and collection
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = mock_cursor
        
        mongodb = MongoDB()
        mongodb.client = mock_client
        
        from pymongo import DESCENDING, ASCENDING
        
        # Test various descending formats
        descending_formats = ["descending", "desc", "DESCENDING", "DESC", "Desc"]
        for sort_dir in descending_formats:
            mock_collection.reset_mock()
            with mongodb.cursor("test_collection", {}, "field", sort_dir):
                pass
            mock_collection.create_index.assert_called_once_with([("field", DESCENDING)])
        
        # Test ascending format (any other string)
        mock_collection.reset_mock()
        with mongodb.cursor("test_collection", {}, "field", "ascending"):
            pass
        mock_collection.create_index.assert_called_once_with([("field", ASCENDING)])
        
        # Test random string defaults to ascending
        mock_collection.reset_mock()
        with mongodb.cursor("test_collection", {}, "field", "random"):
            pass
        mock_collection.create_index.assert_called_once_with([("field", ASCENDING)])
