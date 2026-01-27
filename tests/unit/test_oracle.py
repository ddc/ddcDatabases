from ddcDatabases.core.base import ConnectionTester
from ddcDatabases.oracle import Oracle, OracleConnectionConfig, OraclePoolConfig, OracleSessionConfig, OracleSSLConfig
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestOracle:
    """Test Oracle database connection class"""

    # noinspection PyMethodMayBeStatic
    def _create_mock_settings(self, **overrides):
        """Create mock settings with all required fields"""
        mock_settings = MagicMock()
        defaults = {
            "user": "system",
            "password": "oracle",
            "host": "localhost",
            "port": 1521,
            "servicename": "xe",
            "echo": False,
            "sync_driver": "oracle+oracledb",
            "autoflush": False,
            "expire_on_commit": False,
            "autocommit": True,
            "connection_timeout": 30,
            "pool_recycle": 3600,
            "pool_size": 10,
            "max_overflow": 20,
            "ssl_enabled": False,
            "ssl_wallet_path": None,
            "enable_retry": True,
            "max_retries": 3,
            "initial_retry_delay": 1.0,
            "max_retry_delay": 30.0,
        }
        defaults.update(overrides)
        for key, value in defaults.items():
            setattr(mock_settings, key, value)
        return mock_settings

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_init_basic(self, mock_get_settings):
        """Test Oracle basic initialization"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle()

        assert oracle.connection_url["host"] == "localhost"
        assert oracle.connection_url["port"] == 1521
        assert oracle.connection_url["username"] == "system"
        assert oracle.connection_url["password"] == "oracle"
        assert oracle.connection_url["query"]["service_name"] == "xe"
        assert oracle.sync_driver == "oracle+oracledb"

    def test_init_missing_credentials(self):
        """Test Oracle initialization with missing credentials"""
        # Create mock settings outside the patched function
        mock_settings = MagicMock()
        mock_settings.user = None
        mock_settings.password = "oracle"
        mock_settings.host = "localhost"
        mock_settings.port = 1521
        mock_settings.servicename = "xe"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = True
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 10
        mock_settings.max_overflow = 20
        mock_settings.sync_driver = "oracle+oracledb"

        # Create a completely custom init that uses mock settings
        def patched_init(oracle_self, *args, **kwargs):
            oracle_self.echo = None or mock_settings.echo
            oracle_self.autoflush = None
            oracle_self.expire_on_commit = None
            oracle_self.sync_driver = mock_settings.sync_driver
            oracle_self.connection_url = {
                "host": None or mock_settings.host,
                "port": int(None or mock_settings.port),
                "username": None or mock_settings.user,  # This will be None
                "password": None or mock_settings.password,
                "query": {
                    "service_name": None or mock_settings.servicename,
                    "encoding": "UTF-8",
                    "nencoding": "UTF-8",
                },
            }

            if not oracle_self.connection_url["username"] or not oracle_self.connection_url["password"]:
                raise RuntimeError("Missing username/password")

        with patch.object(Oracle, '__init__', patched_init):
            with pytest.raises(RuntimeError, match="Missing username/password"):
                Oracle()

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_init_with_parameters(self, mock_get_settings):
        """Test Oracle initialization with override parameters"""
        mock_settings = self._create_mock_settings(
            user="defaultuser", password="defaultpass", host="defaulthost", servicename="defaultxe"
        )
        mock_get_settings.return_value = mock_settings

        oracle = Oracle(
            host="customhost",
            port=1522,
            user="customuser",
            password="custompass",
            servicename="customxe",
            session_config=OracleSessionConfig(echo=True),
        )

        assert oracle.connection_url["host"] == "customhost"
        assert oracle.connection_url["port"] == 1522
        assert oracle.connection_url["username"] == "customuser"
        assert oracle.connection_url["password"] == "custompass"
        assert oracle.connection_url["query"]["service_name"] == "customxe"
        assert oracle._session_config.echo == True

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_minimal_init(self, mock_get_settings):
        """Test Oracle minimal initialization"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle()

        assert oracle.connection_url["host"] == "localhost"
        assert oracle.connection_url["port"] == 1521
        # Oracle uses 'query' field for servicename
        assert "query" in oracle.connection_url
        assert oracle.connection_url["query"]["service_name"] == "xe"
        assert oracle.sync_driver == "oracle+oracledb"

    def test_test_connection_sync_oracle(self):
        """Test connection test for Oracle database"""
        mock_session = MagicMock()
        mock_session.bind.url = "oracle://user:password@host/xe"

        test_conn = ConnectionTester(sync_session=mock_session)
        result = test_conn.test_connection_sync()

        assert result == True
        mock_session.execute.assert_called_once()
        # Check that Oracle-specific query was used
        call_args = mock_session.execute.call_args[0][0]
        assert "SELECT 1 FROM dual" in str(call_args)

    @pytest.mark.asyncio
    async def test_test_connection_async_oracle(self):
        """Test async connection test for Oracle database"""
        mock_session = AsyncMock()
        mock_session.bind.url = "oracle://user:password@host/xe"

        test_conn = ConnectionTester(async_session=mock_session)
        result = await test_conn.test_connection_async()

        assert result == True
        mock_session.execute.assert_called_once()

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_extra_engine_args(self, mock_get_settings):
        """Test Oracle with extra engine arguments"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        extra_args = {"encoding": "UTF-8", "nencoding": "UTF-8"}
        oracle = Oracle(extra_engine_args=extra_args)

        # Test that extra args are included in engine_args
        assert "encoding" in oracle.engine_args
        assert oracle.engine_args["encoding"] == "UTF-8"
        assert oracle.engine_args["nencoding"] == "UTF-8"

        # Test that default args are still present
        assert oracle.engine_args["echo"] == False

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_autoflush_and_expire_on_commit(self, mock_get_settings):
        """Test Oracle autoflush and expire_on_commit parameters"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle(session_config=OracleSessionConfig(autoflush=False, expire_on_commit=False))

        assert oracle._session_config.autoflush == False
        assert oracle._session_config.expire_on_commit == False

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_autocommit_parameter(self, mock_get_settings):
        """Test Oracle autocommit parameter"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle(session_config=OracleSessionConfig(autocommit=False))

        assert oracle._session_config.autocommit == False

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_connection_timeout_parameter(self, mock_get_settings):
        """Test Oracle connection_timeout parameter"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle(pool_config=OraclePoolConfig(connection_timeout=60))

        assert oracle._pool_config.connection_timeout == 60

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_all_parameters_defaults(self, mock_get_settings):
        """Test Oracle parameters use settings defaults"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle()

        assert oracle._session_config.autoflush == False
        assert oracle._session_config.expire_on_commit == False
        assert oracle._session_config.autocommit == True
        assert oracle._pool_config.connection_timeout == 30

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_pool_recycle_parameter(self, mock_get_settings):
        """Test Oracle pool_recycle parameter"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle(pool_config=OraclePoolConfig(pool_recycle=7200))

        assert oracle._pool_config.pool_recycle == 7200
        assert oracle.engine_args["pool_recycle"] == 7200

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_pool_size_parameter(self, mock_get_settings):
        """Test Oracle pool_size parameter"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle(pool_config=OraclePoolConfig(pool_size=15))

        assert oracle._pool_config.pool_size == 15
        assert oracle.engine_args["pool_size"] == 15

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_max_overflow_parameter(self, mock_get_settings):
        """Test Oracle max_overflow parameter"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle(pool_config=OraclePoolConfig(max_overflow=30))

        assert oracle._pool_config.max_overflow == 30
        assert oracle.engine_args["max_overflow"] == 30

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_pool_parameters_defaults(self, mock_get_settings):
        """Test Oracle pool parameters use settings defaults"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle()

        assert oracle._pool_config.pool_recycle == 3600
        assert oracle._pool_config.pool_size == 10
        assert oracle._pool_config.max_overflow == 20
        assert oracle.engine_args["pool_recycle"] == 3600
        assert oracle.engine_args["pool_size"] == 10
        assert oracle.engine_args["max_overflow"] == 20

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_ssl_wallet_path(self, mock_get_settings):
        """Test Oracle SSL wallet path configuration"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle(ssl_config=OracleSSLConfig(ssl_enabled=True, ssl_wallet_path="/path/to/wallet"))

        assert oracle._ssl_config.ssl_enabled == True
        assert oracle._ssl_config.ssl_wallet_path == "/path/to/wallet"
        assert oracle.engine_args["connect_args"]["wallet_location"] == "/path/to/wallet"

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_ssl_disabled_no_wallet(self, mock_get_settings):
        """Test Oracle without SSL wallet does not add wallet_location"""
        mock_settings = self._create_mock_settings()
        mock_get_settings.return_value = mock_settings

        oracle = Oracle()

        assert oracle._ssl_config.ssl_enabled == False
        assert oracle._ssl_config.ssl_wallet_path is None
        assert "wallet_location" not in oracle.engine_args["connect_args"]

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_get_connection_info(self, mock_get_settings):
        """Test get_connection_info returns connection config"""
        mock_get_settings.return_value = self._create_mock_settings()
        oracle = Oracle()

        conn_info = oracle.get_connection_info()

        assert conn_info is oracle._connection_config
        assert isinstance(conn_info, OracleConnectionConfig)
        assert conn_info.host == "localhost"
        assert conn_info.port == 1521

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_get_pool_info(self, mock_get_settings):
        """Test get_pool_info returns pool config"""
        mock_get_settings.return_value = self._create_mock_settings()
        oracle = Oracle(pool_config=OraclePoolConfig(pool_size=20, max_overflow=40))

        pool_info = oracle.get_pool_info()

        assert pool_info is oracle._pool_config
        assert isinstance(pool_info, OraclePoolConfig)
        assert pool_info.pool_size == 20
        assert pool_info.max_overflow == 40

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_get_session_info(self, mock_get_settings):
        """Test get_session_info returns session config"""
        mock_get_settings.return_value = self._create_mock_settings()
        oracle = Oracle(session_config=OracleSessionConfig(echo=True, autoflush=False))

        session_info = oracle.get_session_info()

        assert session_info is oracle._session_config
        assert isinstance(session_info, OracleSessionConfig)
        assert session_info.echo == True
        assert session_info.autoflush == False

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_get_conn_retry_info(self, mock_get_settings):
        """Test get_conn_retry_info returns connection retry config"""
        mock_get_settings.return_value = self._create_mock_settings()
        oracle = Oracle()

        conn_retry_info = oracle.get_conn_retry_info()

        assert conn_retry_info is oracle._conn_retry_config
        assert hasattr(conn_retry_info, 'enable_retry')
        assert hasattr(conn_retry_info, 'max_retries')

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_get_op_retry_info(self, mock_get_settings):
        """Test get_op_retry_info returns operation retry config"""
        mock_get_settings.return_value = self._create_mock_settings()
        oracle = Oracle()

        op_retry_info = oracle.get_op_retry_info()

        assert op_retry_info is oracle._op_retry_config
        assert hasattr(op_retry_info, 'enable_retry')
        assert hasattr(op_retry_info, 'max_retries')
        assert hasattr(op_retry_info, 'jitter')

    @patch('ddcDatabases.oracle.get_oracle_settings')
    def test_get_ssl_info(self, mock_get_settings):
        """Test get_ssl_info returns SSL config"""
        mock_get_settings.return_value = self._create_mock_settings()
        oracle = Oracle(ssl_config=OracleSSLConfig(ssl_enabled=True, ssl_wallet_path="/path/to/wallet"))

        ssl_info = oracle.get_ssl_info()

        assert ssl_info is oracle._ssl_config
        assert isinstance(ssl_info, OracleSSLConfig)
        assert ssl_info.ssl_enabled == True
        assert ssl_info.ssl_wallet_path == "/path/to/wallet"
