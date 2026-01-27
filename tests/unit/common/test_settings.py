from ddcDatabases.core.settings import (
    MongoDBSettings,
    MSSQLSettings,
    MySQLSettings,
    OracleSettings,
    PostgreSQLSettings,
    SQLiteSettings,
    get_mongodb_settings,
    get_mssql_settings,
    get_mysql_settings,
    get_oracle_settings,
    get_postgresql_settings,
    get_sqlite_settings,
)
import os
from unittest.mock import patch


class TestSettingsCache:
    """Test cached settings functionality"""

    def test_sqlite_settings_cache(self):
        """Test SQLite settings are cached"""
        # Clear any existing cache
        get_sqlite_settings.cache_clear()

        settings1 = get_sqlite_settings()
        settings2 = get_sqlite_settings()

        # Should return the same instance
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
        with patch.dict(
            os.environ,
            {
                'SQLITE_FILE_PATH': 'tests/data/test.db',
                'SQLITE_ECHO': 'true',
            },
        ):
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
        assert settings.schema == "public"
        assert settings.echo == False
        assert settings.async_driver == "postgresql+asyncpg"
        assert settings.sync_driver == "postgresql+psycopg"
        assert settings.ssl_mode == "disable"
        assert settings.ssl_ca_cert_path is None
        assert settings.ssl_client_cert_path is None
        assert settings.ssl_client_key_path is None

    def test_validate_ssl_mode_valid(self):
        """Test validate_ssl_mode accepts valid modes"""
        for mode in ("disable", "allow", "prefer", "require", "verify-ca", "verify-full"):
            settings = PostgreSQLSettings(ssl_mode=mode)
            assert settings.ssl_mode == mode

    def test_validate_ssl_mode_default(self):
        """Test validate_ssl_mode default is disable"""
        settings = PostgreSQLSettings()
        assert settings.ssl_mode == "disable"

    def test_env_override(self):
        """Test environment variable overrides"""
        with patch.dict(
            os.environ,
            {
                'POSTGRESQL_HOST': 'custom-host',
                'POSTGRESQL_PORT': '9999',
                'POSTGRESQL_USER': 'testuser',
                'POSTGRESQL_PASSWORD': 'testpass',
                'POSTGRESQL_DATABASE': 'testdb',
                'POSTGRESQL_ECHO': 'true',
                'POSTGRESQL_SCHEMA': 'custom_schema',
                'POSTGRESQL_SSL_MODE': 'require',
            },
        ):
            settings = PostgreSQLSettings()
            assert settings.host == 'custom-host'
            assert settings.port == 9999
            assert settings.user == 'testuser'
            assert settings.password == 'testpass'
            assert settings.database == 'testdb'
            assert settings.echo == True
            assert settings.schema == 'custom_schema'
            assert settings.ssl_mode == 'require'


class TestMSSQLSettings:
    """Test MSSQL settings"""

    def test_default_values(self):
        """Test default MSSQL settings values"""
        settings = MSSQLSettings()

        assert settings.host == "localhost"
        assert settings.port == 1433
        assert settings.user == "sa"
        assert settings.password == "sa"
        assert settings.schema == "dbo"
        assert settings.database == "master"
        assert settings.echo == False
        assert settings.pool_size == 25
        assert settings.max_overflow == 50
        assert settings.odbcdriver_version == 18
        assert settings.async_driver == "mssql+aioodbc"
        assert settings.sync_driver == "mssql+pyodbc"
        assert settings.ssl_encrypt == False
        assert settings.ssl_trust_server_certificate == True
        assert settings.ssl_ca_cert_path is None


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
        assert settings.sync_driver == "mysql+mysqldb"
        assert settings.ssl_mode == "DISABLED"
        assert settings.ssl_ca_cert_path is None
        assert settings.ssl_client_cert_path is None
        assert settings.ssl_client_key_path is None

    def test_validate_ssl_mode_valid(self):
        """Test validate_ssl_mode accepts valid MySQL modes"""
        for mode in ("DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"):
            settings = MySQLSettings(ssl_mode=mode)
            assert settings.ssl_mode == mode

    def test_validate_ssl_mode_default(self):
        """Test validate_ssl_mode default is DISABLED"""
        settings = MySQLSettings()
        assert settings.ssl_mode == "DISABLED"


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
        assert settings.driver == "mongodb"
        assert settings.tls_enabled == False
        assert settings.tls_ca_cert_path is None
        assert settings.tls_cert_key_path is None
        assert settings.tls_allow_invalid_certificates == False


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
        assert settings.sync_driver == "oracle+oracledb"
        assert settings.ssl_enabled == False
        assert settings.ssl_wallet_path is None


class TestDotenvLoading:
    """Test dotenv loading functionality"""

    def setup_method(self):
        """Clear all settings caches before each test to ensure isolation"""
        from ddcDatabases.core.settings import (
            get_mongodb_settings,
            get_mssql_settings,
            get_mysql_settings,
            get_oracle_settings,
            get_postgresql_settings,
            get_sqlite_settings,
        )

        get_sqlite_settings.cache_clear()
        get_postgresql_settings.cache_clear()
        get_mssql_settings.cache_clear()
        get_mysql_settings.cache_clear()
        get_mongodb_settings.cache_clear()
        get_oracle_settings.cache_clear()
        # Reset dotenv flag to ensure clean state
        import ddcDatabases.core.settings

        ddcDatabases.core.settings._dotenv_loaded = False

    @patch('ddcDatabases.core.settings._dotenv_loaded', True)
    @patch('ddcDatabases.core.settings.load_dotenv')
    def test_dotenv_not_loaded_if_already_loaded(self, mock_load_dotenv):
        """Test that dotenv is not loaded if already loaded"""
        get_postgresql_settings.cache_clear()

        get_postgresql_settings()
        mock_load_dotenv.assert_not_called()
