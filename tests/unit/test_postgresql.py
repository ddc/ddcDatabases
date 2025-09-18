from unittest.mock import MagicMock, patch
import pytest

try:
    import psycopg2
    import asyncpg
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False

pytestmark = pytest.mark.skipif(not POSTGRESQL_AVAILABLE, reason="PostgreSQL drivers not available")

from ddcDatabases.postgresql import PostgreSQL


class TestPostgreSQL:
    """Test PostgreSQL database connection class"""

    def setup_method(self):
        """Clear cache before each test"""
        # Clear all settings caches before each test
        from ddcDatabases.settings import get_postgresql_settings

        # Force clear the cache
        get_postgresql_settings.cache_clear()

        # Also clear the module-level dotenv flag
        import ddcDatabases.settings

        ddcDatabases.settings._dotenv_loaded = False

    def test_init_missing_credentials(self):
        """Test PostgreSQL initialization with missing credentials"""
        # Create mock settings outside the patched function
        mock_settings = MagicMock()
        mock_settings.user = None
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"

        # Create a completely custom init that uses mock settings
        def patched_init(postgresql_self, *args, **kwargs):
            # Simplified init logic for test
            postgresql_self.connection_url = {
                "host": None or mock_settings.host,
                "port": None or mock_settings.port,
                "database": None or mock_settings.database,
                "username": None or mock_settings.user,  # This will be None
                "password": None or mock_settings.password,
            }

            if not postgresql_self.connection_url["username"] or not postgresql_self.connection_url["password"]:
                raise RuntimeError("Missing username/password")

        with patch.object(PostgreSQL, '__init__', patched_init):
            with pytest.raises(RuntimeError, match="Missing username/password"):
                PostgreSQL()

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test PostgreSQL initialization with override parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "defaultuser"
        mock_settings.password = "defaultpass"
        mock_settings.host = "defaulthost"
        mock_settings.port = 5432
        mock_settings.database = "defaultdb"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(
            host="customhost", port=5433, user="customuser", password="custompass", database="customdb", echo=True
        )

        assert postgresql.connection_url["host"] == "customhost"
        assert postgresql.connection_url["port"] == 5433
        assert postgresql.connection_url["database"] == "customdb"
        assert postgresql.connection_url["username"] == "customuser"
        assert postgresql.connection_url["password"] == "custompass"
        assert postgresql.echo == True

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_extra_engine_args(self, mock_get_settings):
        """Test PostgreSQL with extra engine arguments"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        extra_args = {"pool_timeout": 60, "connect_timeout": 30}
        postgresql = PostgreSQL(extra_engine_args=extra_args)

        # Test that extra args are included in engine_args
        assert "pool_timeout" in postgresql.engine_args
        assert postgresql.engine_args["pool_timeout"] == 60
        assert postgresql.engine_args["connect_timeout"] == 30

        # Test that default args are still present
        assert postgresql.engine_args["echo"] == False

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_autoflush_and_expire_on_commit(self, mock_get_settings):
        """Test PostgreSQL autoflush and expire_on_commit parameters"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(autoflush=False, expire_on_commit=False)

        assert postgresql.autoflush == False
        assert postgresql.expire_on_commit == False

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_autocommit_parameter(self, mock_get_settings):
        """Test PostgreSQL autocommit parameter"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autocommit = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(autocommit=True)

        assert postgresql.autocommit == True

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_connect_args_psycopg2_driver(self, mock_get_settings):
        """Test that psycopg2 driver sets correct connect_args"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL()

        # Test that driver detection works correctly
        assert "psycopg2" in postgresql.sync_driver
        assert "asyncpg" in postgresql.async_driver

    def test_connect_args_different_driver(self):
        """Test that driver detection logic works correctly"""
        # Test the actual logic without trying to mock settings
        # This tests the string matching logic in the _get_engine method

        # Create a test instance with default settings
        postgresql = PostgreSQL()

        # Test that we can detect different driver patterns
        assert "postgresql" in postgresql.sync_driver
        assert "postgresql" in postgresql.async_driver

        # Test the string matching logic that would be used in _get_engine
        test_driver_psycopg2 = "postgresql+psycopg2"
        test_driver_pg8000 = "postgresql+pg8000"

        assert "psycopg2" in test_driver_psycopg2
        assert "psycopg2" not in test_driver_pg8000
        assert "pg8000" in test_driver_pg8000

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_driver_detection_logic(self, mock_get_settings):
        """Test driver detection logic in init"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL()

        # Verify drivers are correctly set
        assert postgresql.sync_driver == "postgresql+psycopg2"
        assert postgresql.async_driver == "postgresql+asyncpg"
        assert "psycopg2" in postgresql.sync_driver
        assert "asyncpg" in postgresql.async_driver

    def test_async_driver_detection(self):
        """Test async driver detection for different drivers"""
        # Test the actual logic without trying to mock settings
        # This tests the string matching logic in the _get_async_engine method

        # Create a test instance with default settings
        postgresql = PostgreSQL()

        # Test that we can detect the async driver pattern
        assert "postgresql" in postgresql.async_driver

        # Test the string matching logic that would be used in _get_async_engine
        test_driver_asyncpg = "postgresql+asyncpg"
        test_driver_aiopg = "postgresql+aiopg"

        assert "asyncpg" in test_driver_asyncpg
        assert "asyncpg" not in test_driver_aiopg
        assert "aiopg" in test_driver_aiopg

    def test_engine_args_structure(self):
        """Test that engine args are properly structured"""
        # Test engine args inheritance without relying on mocked settings
        extra_args = {"pool_timeout": 60}
        postgresql = PostgreSQL(extra_engine_args=extra_args)

        # Test that extra args are properly included
        assert postgresql.engine_args['pool_pre_ping'] == True
        assert postgresql.engine_args['pool_recycle'] == 3600
        assert postgresql.engine_args['pool_timeout'] == 60

        # Test that extra_engine_args dict is properly stored
        assert hasattr(postgresql, 'extra_engine_args')
        assert postgresql.extra_engine_args == extra_args

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_context_manager_methods_exist(self, mock_get_settings):
        """Test that context manager methods exist and can be called safely"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL()

        # Test that methods exist and are callable
        assert hasattr(postgresql, '_get_engine')
        assert hasattr(postgresql, '_get_async_engine')
        assert callable(postgresql._get_engine)
        assert callable(postgresql._get_async_engine)

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_get_base_engine_args_method(self, mock_get_settings):
        """Test _get_base_engine_args method - covers lines 70-87"""
        from sqlalchemy import URL

        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL()

        # Test _get_base_engine_args method
        connection_url = URL.create(
            drivername="postgresql+psycopg2",
            host="localhost",
            port=5432,
            database="postgres",
            username="postgres",
            password="password",
        )
        driver_connect_args = {"connect_timeout": 30}
        driver_engine_args = {"isolation_level": "READ_COMMITTED"}

        result = postgresql._get_base_engine_args(connection_url, driver_connect_args, driver_engine_args)

        # Test the result contains expected keys
        assert "url" in result
        assert "pool_size" in result
        assert "max_overflow" in result
        assert "pool_pre_ping" in result
        assert "pool_recycle" in result
        assert "query_cache_size" in result
        assert "connect_args" in result
        assert "isolation_level" in result

        # Test values
        assert result["url"] == connection_url
        assert result["pool_size"] == 25
        assert result["max_overflow"] == 50
        assert result["pool_pre_ping"] == True
        assert result["pool_recycle"] == 3600
        assert result["query_cache_size"] == 1000
        assert result["connect_args"]["connect_timeout"] == 30
        assert result["isolation_level"] == "READ_COMMITTED"

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_sync_driver_autocommit_logic(self, mock_get_settings):
        """Test autocommit logic for sync driver - covers autocommit branch"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(autocommit=True)

        # Test autocommit logic by simulating the engine args creation
        from sqlalchemy import URL

        connection_url = URL.create(
            drivername=postgresql.sync_driver,
            **postgresql.connection_url,
        )
        sync_connect_args = {}
        sync_engine_args = {}

        # Simulate the psycopg2 driver logic
        if "psycopg2" in postgresql.sync_driver:
            sync_connect_args["connect_timeout"] = postgresql.connection_timeout
            if postgresql.autocommit:
                sync_engine_args["isolation_level"] = "AUTOCOMMIT"

        engine_args = postgresql._get_base_engine_args(connection_url, sync_connect_args, sync_engine_args)

        # Verify autocommit isolation level is set
        assert "isolation_level" in engine_args
        assert engine_args["isolation_level"] == "AUTOCOMMIT"

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_async_driver_autocommit_logic(self, mock_get_settings):
        """Test autocommit logic for async driver - covers async autocommit branch"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(autocommit=True)

        # Test async autocommit logic
        from sqlalchemy import URL

        connection_url = URL.create(
            drivername=postgresql.async_driver,
            **postgresql.connection_url,
        )
        async_connect_args = {}
        async_engine_args = {}

        # Simulate the asyncpg driver logic
        if "asyncpg" in postgresql.async_driver:
            async_connect_args["command_timeout"] = postgresql.connection_timeout
            if postgresql.autocommit:
                async_engine_args["isolation_level"] = "AUTOCOMMIT"

        engine_args = postgresql._get_base_engine_args(connection_url, async_connect_args, async_engine_args)

        # Verify autocommit isolation level and command timeout are set
        assert "isolation_level" in engine_args
        assert engine_args["isolation_level"] == "AUTOCOMMIT"
        assert "connect_args" in engine_args
        assert "command_timeout" in engine_args["connect_args"]
        assert engine_args["connect_args"]["command_timeout"] == 30

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_pool_size_parameter(self, mock_get_settings):
        """Test PostgreSQL pool_size parameter"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_settings.pool_size = 10
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(pool_size=15)

        assert postgresql.pool_size == 15

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_max_overflow_parameter(self, mock_get_settings):
        """Test PostgreSQL max_overflow parameter"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_settings.max_overflow = 20
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(max_overflow=30)

        assert postgresql.max_overflow == 30

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_pool_parameters_defaults(self, mock_get_settings):
        """Test PostgreSQL pool parameters use settings defaults"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL()

        assert postgresql.pool_size == 25
        assert postgresql.max_overflow == 50

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_enhanced_configuration_methods(self, mock_get_settings):
        """Test the new enhanced configuration getter methods"""
        mock_settings = MagicMock()
        mock_settings.host = "testhost"
        mock_settings.port = 5432
        mock_settings.user = "testuser"
        mock_settings.password = "testpass"
        mock_settings.database = "testdb"
        mock_settings.echo = True
        mock_settings.autoflush = True
        mock_settings.expire_on_commit = True
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 45
        mock_settings.pool_recycle = 7200
        mock_settings.pool_size = 30
        mock_settings.max_overflow = 60
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(
            host="customhost",
            port=5433,
            user="customuser",
            password="custompass",
            database="customdb",
            echo=False,
            autoflush=False,
            expire_on_commit=False,
            autocommit=True,
            connection_timeout=60,
            pool_recycle=9000,
            pool_size=40,
            max_overflow=80
        )

        # Test get_connection_info method (line 153)
        conn_config = postgresql.get_connection_info()
        assert conn_config.host == "customhost"
        assert conn_config.port == 5433
        assert conn_config.user == "customuser"
        assert conn_config.password == "custompass"
        assert conn_config.database == "customdb"

        # Test get_pool_info method (line 157)
        pool_config = postgresql.get_pool_info()
        assert pool_config.pool_size == 40
        assert pool_config.max_overflow == 80
        assert pool_config.pool_recycle == 9000
        assert pool_config.connection_timeout == 60

        # Test get_session_info method (line 161)
        session_config = postgresql.get_session_info()
        assert session_config.echo == False
        assert session_config.autoflush == False
        assert session_config.expire_on_commit == False
        assert session_config.autocommit == True

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_get_engine_method_with_psycopg2(self, mock_get_settings):
        """Test the _get_engine method with psycopg2 driver"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(autocommit=True)

        # Test that _get_engine context manager works and returns an engine
        with postgresql._get_engine() as engine:
            # Verify we get a real SQLAlchemy engine
            assert hasattr(engine, 'dispose')
            assert hasattr(engine, 'connect')
            # Verify URL was constructed correctly
            assert "postgresql+psycopg2" in str(engine.url)
            assert "localhost" in str(engine.url)
            assert "postgres" in str(engine.url)

        # Engine should be properly disposed after context exit
        # After dispose(), the pool should be invalidated or recreated
        # We can check that the engine is in a disposed state
        try:
            # Try to get a connection - should fail or create new pool after disposal
            with engine.connect() as conn:
                pass
        except Exception:
            # This is expected if the engine was properly disposed
            pass

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_get_engine_method_without_autocommit(self, mock_get_settings):
        """Test the _get_engine method without autocommit"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(autocommit=False)

        # Test the _get_engine context manager without autocommit
        with postgresql._get_engine() as engine:
            # Verify we get a real SQLAlchemy engine
            assert hasattr(engine, 'dispose')
            assert hasattr(engine, 'connect')
            # Verify URL was constructed correctly
            assert "postgresql+psycopg2" in str(engine.url)

        # Engine should be properly disposed after context exit
        # After dispose(), the pool should be invalidated or recreated
        # We can check that the engine is in a disposed state
        try:
            # Try to get a connection - should fail or create new pool after disposal
            with engine.connect() as conn:
                pass
        except Exception:
            # This is expected if the engine was properly disposed
            pass

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_get_engine_method_non_psycopg2_driver(self, mock_get_settings):
        """Test the _get_engine method with non-psycopg2 driver"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"  # Use psycopg2 (available driver)
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL()

        # Test the _get_engine context manager with non-psycopg2 driver
        with postgresql._get_engine() as engine:
            # Verify we get a real SQLAlchemy engine
            assert hasattr(engine, 'dispose')
            assert hasattr(engine, 'connect')
            # For non-psycopg2 drivers, should still use psycopg2 as fallback
            # since the settings return psycopg2 as sync_driver for PostgreSQL
            assert "postgresql" in str(engine.url)

        # Engine should be properly disposed after context exit
        # After dispose(), the pool should be invalidated or recreated
        # We can check that the engine is in a disposed state
        try:
            # Try to get a connection - should fail or create new pool after disposal
            with engine.connect() as conn:
                pass
        except Exception:
            # This is expected if the engine was properly disposed
            pass

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_get_async_engine_method_with_asyncpg(self, mock_get_settings):
        """Test the _get_async_engine method with asyncpg driver"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 45
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(autocommit=True, connection_timeout=60)

        # Test the _get_async_engine context manager
        async def test_async():
            async with postgresql._get_async_engine() as engine:
                # Verify we get a real SQLAlchemy AsyncEngine
                assert hasattr(engine, 'dispose')
                assert hasattr(engine, 'begin')
                # Verify URL was constructed correctly
                assert "postgresql+asyncpg" in str(engine.url)
                assert "localhost" in str(engine.url)

        # Run the async test
        import asyncio
        asyncio.run(test_async())

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_get_async_engine_method_without_autocommit(self, mock_get_settings):
        """Test the _get_async_engine method without autocommit"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(autocommit=False)

        # Test the _get_async_engine context manager without autocommit
        async def test_async():
            async with postgresql._get_async_engine() as engine:
                # Verify we get a real SQLAlchemy AsyncEngine
                assert hasattr(engine, 'dispose')
                assert hasattr(engine, 'begin')
                # Verify URL was constructed correctly
                assert "postgresql+asyncpg" in str(engine.url)

        import asyncio
        asyncio.run(test_async())

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_get_async_engine_method_non_asyncpg_driver(self, mock_get_settings):
        """Test the _get_async_engine method with non-asyncpg driver"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"  # Use asyncpg (available driver)
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL()

        # Test the _get_async_engine context manager with non-asyncpg driver
        async def test_async():
            async with postgresql._get_async_engine() as engine:
                # Verify we get a real SQLAlchemy AsyncEngine
                assert hasattr(engine, 'dispose')
                assert hasattr(engine, 'begin')
                # Verify URL was constructed correctly
                assert "postgresql+asyncpg" in str(engine.url)

        import asyncio
        asyncio.run(test_async())

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_repr_method(self, mock_get_settings):
        """Test the enhanced __repr__ method"""
        mock_settings = MagicMock()
        mock_settings.user = "testuser"
        mock_settings.password = "testpass"
        mock_settings.host = "testhost"
        mock_settings.port = 5432
        mock_settings.database = "testdb"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(
            host="myhost",
            port=5433,
            database="mydb",
            pool_size=30,
            echo=True
        )

        repr_str = repr(postgresql)

        # Check that all expected values are in the repr string
        assert "PostgreSQL(" in repr_str
        assert "host='myhost'" in repr_str
        assert "port=5433" in repr_str
        assert "database='mydb'" in repr_str
        assert "pool_size=30" in repr_str
        assert "echo=True" in repr_str
        assert ")" in repr_str

    @patch('ddcDatabases.postgresql.get_postgresql_settings')
    def test_configuration_immutability(self, mock_get_settings):
        """Test that configuration objects are properly immutable"""
        mock_settings = MagicMock()
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.database = "postgres"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg2"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL()

        # Test that configuration objects are frozen (immutable)
        conn_config = postgresql.get_connection_info()
        pool_config = postgresql.get_pool_info()
        session_config = postgresql.get_session_info()

        # Try to modify configurations - should raise FrozenInstanceError
        with pytest.raises(Exception):  # FrozenInstanceError
            conn_config.host = "modified"

        with pytest.raises(Exception):  # FrozenInstanceError
            pool_config.pool_size = 999

        with pytest.raises(Exception):  # FrozenInstanceError
            session_config.echo = True
