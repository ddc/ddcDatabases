"""Tests for MSSQL SSL configuration."""

import pytest


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
