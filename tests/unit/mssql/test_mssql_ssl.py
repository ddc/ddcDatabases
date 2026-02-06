"""Tests for MSSQL SSL configuration."""

import os
import pytest
from unittest.mock import MagicMock, patch


class TestMSSQLSSLConfig:
    """Test MSSQL SSL configuration validation."""

    def test_default_values_are_none(self):
        """Test MSSQL SSL config default values are None."""
        from ddcDatabases.mssql import MSSQLSSLConfig

        config = MSSQLSSLConfig()
        assert config.ssl_encrypt is None
        assert config.ssl_trust_server_certificate is None
        assert config.ssl_ca_cert_path is None

    def test_ssl_encrypt_enabled(self):
        """Test MSSQL SSL encryption enabled."""
        from ddcDatabases.mssql import MSSQLSSLConfig

        config = MSSQLSSLConfig(ssl_encrypt=True, ssl_trust_server_certificate=False)
        assert config.ssl_encrypt is True
        assert config.ssl_trust_server_certificate is False

    def test_ssl_config_with_ca_cert(self):
        """Test MSSQL SSL config with CA certificate."""
        from ddcDatabases.mssql import MSSQLSSLConfig

        config = MSSQLSSLConfig(
            ssl_encrypt=True,
            ssl_trust_server_certificate=False,
            ssl_ca_cert_path="/path/to/ca.pem",
        )
        assert config.ssl_ca_cert_path == "/path/to/ca.pem"

    def test_ssl_config_immutability(self):
        """Test that SSL config is immutable (frozen)."""
        from ddcDatabases.mssql import MSSQLSSLConfig

        config = MSSQLSSLConfig(ssl_encrypt=True)
        with pytest.raises(AttributeError):
            config.ssl_encrypt = False  # noqa

    def test_ssl_encrypt_disabled(self):
        """Test MSSQL SSL encryption disabled."""
        from ddcDatabases.mssql import MSSQLSSLConfig

        config = MSSQLSSLConfig(ssl_encrypt=False, ssl_trust_server_certificate=True)
        assert config.ssl_encrypt is False
        assert config.ssl_trust_server_certificate is True

    def test_trust_server_certificate_only(self):
        """Test MSSQL SSL config with trust server certificate only."""
        from ddcDatabases.mssql import MSSQLSSLConfig

        config = MSSQLSSLConfig(ssl_trust_server_certificate=True)
        assert config.ssl_trust_server_certificate is True
        assert config.ssl_encrypt is None

    def test_ssl_encrypt_with_trust_cert(self):
        """Test MSSQL SSL encrypt with trust certificate combination."""
        from ddcDatabases.mssql import MSSQLSSLConfig

        config = MSSQLSSLConfig(ssl_encrypt=True, ssl_trust_server_certificate=True)
        assert config.ssl_encrypt is True
        assert config.ssl_trust_server_certificate is True


class TestMSSQLSSLConnectionURL:
    """Test that MSSQL SSL cert paths are correctly added to connection URL."""

    # noinspection PyMethodMayBeStatic
    def setup_method(self):
        """Clear cache before each test."""
        from ddcDatabases.core.settings import get_mssql_settings

        get_mssql_settings.cache_clear()

        import ddcDatabases.core.settings

        ddcDatabases.core.settings._dotenv_loaded = False

    @patch("ddcDatabases.mssql.get_mssql_settings")
    def test_ssl_ca_cert_path_in_connection_url(self, mock_get_settings):
        """Test that ssl_ca_cert_path is added to connection URL query as ServerCertificate."""
        from ddcDatabases.mssql import MSSQL, MSSQLSSLConfig

        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.database = "master"
        mock_settings.schema = "dbo"
        mock_settings.echo = False
        mock_settings.autoflush = True
        mock_settings.expire_on_commit = True
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.odbcdriver_version = 18
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_settings.ssl_encrypt = False
        mock_settings.ssl_trust_server_certificate = True
        mock_settings.ssl_ca_cert_path = None
        mock_settings.connection_enable_retry = None
        mock_settings.connection_max_retries = None
        mock_settings.connection_initial_retry_delay = None
        mock_settings.connection_max_retry_delay = None
        mock_settings.operation_enable_retry = None
        mock_settings.operation_max_retries = None
        mock_settings.operation_initial_retry_delay = None
        mock_settings.operation_max_retry_delay = None
        mock_settings.operation_jitter = None
        mock_get_settings.return_value = mock_settings

        mssql = MSSQL(
            ssl_config=MSSQLSSLConfig(
                ssl_encrypt=True,
                ssl_trust_server_certificate=False,
                ssl_ca_cert_path="/path/to/ca.pem",
            )
        )

        assert mssql.connection_url["query"]["ServerCertificate"] == "/path/to/ca.pem"
        assert mssql.connection_url["query"]["Encrypt"] == "yes"
        assert mssql.connection_url["query"]["TrustServerCertificate"] == "no"

    @patch("ddcDatabases.mssql.get_mssql_settings")
    def test_ssl_no_ca_cert_path_no_server_certificate_key(self, mock_get_settings):
        """Test that ServerCertificate is not in query when ssl_ca_cert_path is None."""
        from ddcDatabases.mssql import MSSQL

        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.database = "master"
        mock_settings.schema = "dbo"
        mock_settings.echo = False
        mock_settings.autoflush = True
        mock_settings.expire_on_commit = True
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.odbcdriver_version = 18
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_settings.ssl_encrypt = False
        mock_settings.ssl_trust_server_certificate = True
        mock_settings.ssl_ca_cert_path = None
        mock_settings.connection_enable_retry = None
        mock_settings.connection_max_retries = None
        mock_settings.connection_initial_retry_delay = None
        mock_settings.connection_max_retry_delay = None
        mock_settings.operation_enable_retry = None
        mock_settings.operation_max_retries = None
        mock_settings.operation_initial_retry_delay = None
        mock_settings.operation_max_retry_delay = None
        mock_settings.operation_jitter = None
        mock_get_settings.return_value = mock_settings

        mssql = MSSQL()

        assert "ServerCertificate" not in mssql.connection_url["query"]

    @patch("ddcDatabases.mssql.get_mssql_settings")
    def test_ssl_ca_cert_path_falls_back_to_settings(self, mock_get_settings):
        """Test that ssl_ca_cert_path falls back to settings when not provided in ssl_config."""
        from ddcDatabases.mssql import MSSQL

        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 1433
        mock_settings.user = "sa"
        mock_settings.password = "password"
        mock_settings.database = "master"
        mock_settings.schema = "dbo"
        mock_settings.echo = False
        mock_settings.autoflush = True
        mock_settings.expire_on_commit = True
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.odbcdriver_version = 18
        mock_settings.sync_driver = "mssql+pyodbc"
        mock_settings.async_driver = "mssql+aioodbc"
        mock_settings.ssl_encrypt = False
        mock_settings.ssl_trust_server_certificate = True
        mock_settings.ssl_ca_cert_path = "/settings/path/to/ca.pem"
        mock_settings.connection_enable_retry = None
        mock_settings.connection_max_retries = None
        mock_settings.connection_initial_retry_delay = None
        mock_settings.connection_max_retry_delay = None
        mock_settings.operation_enable_retry = None
        mock_settings.operation_max_retries = None
        mock_settings.operation_initial_retry_delay = None
        mock_settings.operation_max_retry_delay = None
        mock_settings.operation_jitter = None
        mock_get_settings.return_value = mock_settings

        mssql = MSSQL()

        assert mssql._ssl_config.ssl_ca_cert_path == "/settings/path/to/ca.pem"
        assert mssql.connection_url["query"]["ServerCertificate"] == "/settings/path/to/ca.pem"


class TestMSSQLSSLEnvVars:
    """Test that MSSQL SSL settings are correctly read from environment variables."""

    # noinspection PyMethodMayBeStatic
    def setup_method(self):
        """Clear cache before each test."""
        from ddcDatabases.core.settings import get_mssql_settings

        get_mssql_settings.cache_clear()

        import ddcDatabases.core.settings

        ddcDatabases.core.settings._dotenv_loaded = False

    def test_ssl_ca_cert_path_from_env_var(self):
        """Test that MSSQL_SSL_CA_CERT_PATH env var is read by settings."""
        from ddcDatabases.core.settings import MSSQLSettings

        with patch.dict(
            os.environ,
            {
                "MSSQL_SSL_CA_CERT_PATH": "/env/path/to/ca.pem",
                "MSSQL_SSL_ENCRYPT": "true",
                "MSSQL_SSL_TRUST_SERVER_CERTIFICATE": "false",
            },
        ):
            settings = MSSQLSettings()
            assert settings.ssl_ca_cert_path == "/env/path/to/ca.pem"
            assert settings.ssl_encrypt is True
            assert settings.ssl_trust_server_certificate is False
