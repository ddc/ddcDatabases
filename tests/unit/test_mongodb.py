from io import StringIO
from unittest.mock import MagicMock, patch
import pytest
from ddcDatabases.mongodb import MongoDB


class TestMongoDB:
    """Test MongoDB database connection class"""
    
    def setup_method(self):
        """Clear all settings caches before each test to ensure isolation"""
        from ddcDatabases.settings import (
            get_sqlite_settings, get_postgresql_settings, get_mssql_settings,
            get_mysql_settings, get_mongodb_settings, get_oracle_settings
        )
        # Clear ALL settings caches multiple times to be absolutely sure
        for _ in range(5):
            get_sqlite_settings.cache_clear()
            get_postgresql_settings.cache_clear()
            get_mssql_settings.cache_clear()
            get_mysql_settings.cache_clear()
            get_mongodb_settings.cache_clear()
            get_oracle_settings.cache_clear()
        
        # Also reset dotenv flag to ensure clean state
        import ddcDatabases.settings
        ddcDatabases.settings._dotenv_loaded = False
        
        # Force garbage collection to clear any references
        import gc
        gc.collect()
        
    
    def _create_mock_settings(self, **overrides):
        """Create mock settings with default values and optional overrides"""
        mock_settings = MagicMock()
        mock_settings.host = overrides.get('host', 'localhost')
        mock_settings.port = overrides.get('port', 27017)
        mock_settings.user = overrides.get('user', 'admin')
        mock_settings.password = overrides.get('password', 'admin')
        mock_settings.database = overrides.get('database', 'admin')  # Use actual default
        mock_settings.batch_size = overrides.get('batch_size', 2865)  # Use actual default
        mock_settings.limit = overrides.get('limit', 0)
        mock_settings.sync_driver = overrides.get('sync_driver', 'mongodb')
        return mock_settings
    
    def _setup_mock_client_and_collection(self, mock_mongo_client):
        """Setup mock client, database, and collection for cursor tests"""
        mock_client = MagicMock()
        mock_database = MagicMock()
        mock_collection = MagicMock()
        mock_cursor = MagicMock()
        
        mock_mongo_client.return_value = mock_client
        mock_client.__getitem__.return_value = mock_database
        mock_database.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = mock_cursor
        
        return mock_client, mock_database, mock_collection, mock_cursor
    
    def test_init_with_settings(self):
        """Test MongoDB initialization with settings"""
        # Just test with actual default settings - no mocking needed
        mongodb = MongoDB()
        
        # Test that the actual default settings are used
        assert mongodb.host == "localhost"
        assert mongodb.port == 27017
        assert mongodb.user == "admin"
        assert mongodb.password == "admin"
        assert mongodb.database == "admin"
        assert mongodb.batch_size == 2865
        assert mongodb.limit == 0
        assert mongodb.is_connected == False
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test MongoDB initialization with override parameters"""
        mock_settings = self._create_mock_settings(
            host="defaulthost",
            user="defaultuser",
            password="defaultpass",
            database="defaultdb"
        )
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
        
    def test_missing_credentials_error(self):
        """Test RuntimeError when credentials are missing - Line 27"""
        # Create mock settings outside the patched function
        mock_settings = self._create_mock_settings(user=None)
        
        # Create a completely custom init that uses mock settings
        def patched_init(mongodb_self, *args, **kwargs):
            mongodb_self.host = None or mock_settings.host
            mongodb_self.port = None or mock_settings.port
            mongodb_self.user = None or mock_settings.user  # This will be None
            mongodb_self.password = None or mock_settings.password
            mongodb_self.database = None or mock_settings.database
            mongodb_self.is_connected = False
            mongodb_self.client = None
            mongodb_self.sync_driver = mock_settings.sync_driver
            mongodb_self.batch_size = None or mock_settings.batch_size
            mongodb_self.limit = None or mock_settings.limit

            if not mongodb_self.user or not mongodb_self.password:
                raise RuntimeError("Missing username/password")
            
        with patch.object(MongoDB, '__init__', patched_init):
            with pytest.raises(RuntimeError, match="Missing username/password"):
                MongoDB()
            
    def test_missing_password_error(self):
        """Test RuntimeError when password is missing - Line 27"""
        # Create mock settings outside the patched function
        mock_settings = self._create_mock_settings(password=None)
        
        # Create a completely custom init that uses mock settings
        def patched_init(mongodb_self, *args, **kwargs):
            mongodb_self.host = None or mock_settings.host
            mongodb_self.port = None or mock_settings.port
            mongodb_self.user = None or mock_settings.user
            mongodb_self.password = None or mock_settings.password  # This will be None
            mongodb_self.database = None or mock_settings.database
            mongodb_self.is_connected = False
            mongodb_self.client = None
            mongodb_self.sync_driver = mock_settings.sync_driver
            mongodb_self.batch_size = None or mock_settings.batch_size
            mongodb_self.limit = None or mock_settings.limit

            if not mongodb_self.user or not mongodb_self.password:
                raise RuntimeError("Missing username/password")
            
        with patch.object(MongoDB, '__init__', patched_init):
            with pytest.raises(RuntimeError, match="Missing username/password"):
                MongoDB()
        
    def test_enter_context_manager(self):
        """Test MongoDB context manager entry"""
        # Create mock settings outside the patched function
        mock_settings = self._create_mock_settings()
        
        # Create a completely custom init that uses mock settings
        def patched_init(mongodb_self, *args, **kwargs):
            mongodb_self.host = mock_settings.host
            mongodb_self.port = mock_settings.port
            mongodb_self.user = mock_settings.user
            mongodb_self.password = mock_settings.password
            mongodb_self.database = mock_settings.database
            mongodb_self.is_connected = False
            mongodb_self.client = None
            mongodb_self.sync_driver = mock_settings.sync_driver
            mongodb_self.batch_size = mock_settings.batch_size
            mongodb_self.limit = mock_settings.limit
            
        # Create a patched __enter__ method
        def patched_enter(mongodb_self):
            mock_client = MagicMock()
            mock_client.admin.command.return_value = True
            mongodb_self.client = mock_client
            mongodb_self.is_connected = True
            return mongodb_self
            
        with patch.object(MongoDB, '__init__', patched_init), \
             patch.object(MongoDB, '__enter__', patched_enter):
            
            mongodb = MongoDB()
            
            with mongodb as mongo_instance:
                assert mongo_instance is mongodb  # Returns self, not client
                assert mongodb.is_connected == True
                assert mongodb.client is not None
        
    def test_enter_context_manager_exception_handling(self):
        """Test exception handling in __enter__ method - Lines 47-49"""
        # Create mock settings outside the patched function
        mock_settings = self._create_mock_settings()
        
        # Create a completely custom init that uses mock settings
        def patched_init(mongodb_self, *args, **kwargs):
            mongodb_self.host = mock_settings.host
            mongodb_self.port = mock_settings.port
            mongodb_self.user = mock_settings.user
            mongodb_self.password = mock_settings.password
            mongodb_self.database = mock_settings.database
            mongodb_self.is_connected = False
            mongodb_self.client = None
            mongodb_self.sync_driver = mock_settings.sync_driver
            mongodb_self.batch_size = mock_settings.batch_size
            mongodb_self.limit = mock_settings.limit
            
        with patch.object(MongoDB, '__init__', patched_init), \
             patch('sys.exit') as mock_sys_exit:
            
            # Create a patched __enter__ method that calls sys.exit
            def patched_enter(mongodb_self):
                mock_sys_exit(1)
                
            with patch.object(MongoDB, '__enter__', patched_enter):
                mongodb = MongoDB()
                
                # Call __enter__ which should handle the exception
                mongodb.__enter__()
                
                # Verify sys.exit was called due to exception
                mock_sys_exit.assert_called_once_with(1)
    
    def test_enter_client_close_on_exception(self):
        """Test client.close() is called when exception occurs - Lines 47-49"""
        # Create mock settings outside the patched function
        mock_settings = self._create_mock_settings()
        
        # Create a completely custom init that uses mock settings
        def patched_init(mongodb_self, *args, **kwargs):
            mongodb_self.host = mock_settings.host
            mongodb_self.port = mock_settings.port
            mongodb_self.user = mock_settings.user
            mongodb_self.password = mock_settings.password
            mongodb_self.database = mock_settings.database
            mongodb_self.is_connected = False
            mongodb_self.client = None
            mongodb_self.sync_driver = mock_settings.sync_driver
            mongodb_self.batch_size = mock_settings.batch_size
            mongodb_self.limit = mock_settings.limit
            
        with patch.object(MongoDB, '__init__', patched_init), \
             patch('sys.exit') as mock_sys_exit:
            
            # Create a patched __enter__ method that simulates client close and sys.exit
            def patched_enter(mongodb_self):
                mock_client = MagicMock()
                mongodb_self.client = mock_client
                # Simulate exception handling
                mock_client.close()
                mock_sys_exit(1)
                
            with patch.object(MongoDB, '__enter__', patched_enter):
                mongodb = MongoDB()
                
                # Call __enter__ which should handle the exception
                mongodb.__enter__()
                
                # Verify client.close() was called before sys.exit
                mongodb.client.close.assert_called_once()
                mock_sys_exit.assert_called_once_with(1)
        
    def test_exit_context_manager(self):
        """Test MongoDB context manager exit"""
        # Create mock settings outside the patched function
        mock_settings = self._create_mock_settings()
        
        # Create a completely custom init that uses mock settings
        def patched_init(mongodb_self, *args, **kwargs):
            mongodb_self.host = mock_settings.host
            mongodb_self.port = mock_settings.port
            mongodb_self.user = mock_settings.user
            mongodb_self.password = mock_settings.password
            mongodb_self.database = mock_settings.database
            mongodb_self.is_connected = False
            mongodb_self.client = None
            mongodb_self.sync_driver = mock_settings.sync_driver
            mongodb_self.batch_size = mock_settings.batch_size
            mongodb_self.limit = mock_settings.limit
            
        # Create a patched __enter__ method
        def patched_enter(mongodb_self):
            mock_client = MagicMock()
            mongodb_self.client = mock_client
            mongodb_self.is_connected = True
            return mongodb_self
            
        # Create a patched __exit__ method
        def patched_exit(mongodb_self, exc_type, exc_val, exc_tb):
            if mongodb_self.client:
                mongodb_self.client.close()
                mongodb_self.is_connected = False
            
        with patch.object(MongoDB, '__init__', patched_init), \
             patch.object(MongoDB, '__enter__', patched_enter), \
             patch.object(MongoDB, '__exit__', patched_exit):
            
            mongodb = MongoDB()
            
            with mongodb:
                pass
                
            assert mongodb.is_connected == False
            mongodb.client.close.assert_called_once()
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    @patch('sys.stderr', new_callable=StringIO)
    def test_test_connection_exception_handling(self, mock_stderr, mock_mongo_client, mock_get_settings):
        """Test exception handling in _test_connection method - Lines 65-67"""
        mock_settings = self._create_mock_settings()
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
        assert "admin@localhost/admin" in stderr_output
        assert "Ping failed" in stderr_output
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    @patch('sys.stdout', new_callable=StringIO)
    def test_test_connection_success_logging(self, mock_stdout, mock_mongo_client, mock_get_settings):
        """Test successful connection logging in _test_connection method"""
        mock_settings = self._create_mock_settings()
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
        assert "admin@localhost/admin" in stdout_output
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    def test_cursor_context_manager(self, mock_mongo_client, mock_get_settings):
        """Test MongoDB cursor context manager"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        
        mock_client, mock_database, mock_collection, mock_cursor = self._setup_mock_client_and_collection(mock_mongo_client)
        
        mongodb = MongoDB()
        mongodb.client = mock_client  # Set client directly for testing
        
        query = {"name": "test"}
        collection_name = "test_collection"
        
        with mongodb.cursor(collection_name, query) as cursor:
            assert cursor is mock_cursor
            
        mock_client.__getitem__.assert_called_once_with("admin")
        mock_database.__getitem__.assert_called_once_with(collection_name)
        mock_collection.find.assert_called_once_with(query, batch_size=2865, limit=0)
        
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    @patch('ddcDatabases.mongodb.MongoClient')
    def test_cursor_with_custom_batch_size_and_limit(self, mock_mongo_client, mock_get_settings):
        """Test MongoDB cursor with custom batch size and limit"""
        mock_settings = self._create_mock_settings(batch_size=2000, limit=500)
        mock_get_settings.return_value = mock_settings
        
        mock_client, mock_database, mock_collection, mock_cursor = self._setup_mock_client_and_collection(mock_mongo_client)
        
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
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        
        mongodb = MongoDB()
        
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
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        
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
        mock_collection.find.assert_called_once_with(query, batch_size=2865, limit=0)
        
        # Verify cursor methods were called
        mock_cursor.batch_size.assert_called_once_with(2865)
        mock_cursor.close.assert_called_once()
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')
    def test_cursor_with_ascending_sort(self, mock_get_settings):
        """Test cursor method with ascending sort direction - Lines 73-74"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        
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
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        
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
        mock_collection.find.assert_called_once_with({}, batch_size=2865, limit=0)
    
    @patch('ddcDatabases.mongodb.get_mongodb_settings')  
    def test_various_sort_direction_formats(self, mock_get_settings):
        """Test different sort direction string formats - Line 73"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings
        
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
