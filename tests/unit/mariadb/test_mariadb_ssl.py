"""Tests for MariaDB SSL configuration (MySQL alias).

Note: MariaDB uses MySQL driver and SSL configuration, these tests verify
the MariaDB aliases work correctly.
"""

import pytest


class TestMariaDBSSLConfig:
    """Test MariaDB SSL configuration validation (MySQL alias)."""

    def test_mariadb_ssl_config_is_mysql_alias(self):
        """Test that MariaDBSSLConfig is an alias for MySQLSSLConfig."""
        from ddcdatabases import MariaDBSSLConfig
        from ddcdatabases.mysql import MySQLSSLConfig

        assert MariaDBSSLConfig is MySQLSSLConfig

    def test_valid_ssl_modes(self):
        """Test all valid MariaDB SSL modes."""
        from ddcdatabases import MariaDBSSLConfig

        valid_modes = ["DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"]
        for mode in valid_modes:
            config = MariaDBSSLConfig(ssl_mode=mode)
            assert config.ssl_mode == mode

    def test_invalid_ssl_mode_raises_error(self):
        """Test that invalid SSL mode raises ValueError."""
        from ddcdatabases import MariaDBSSLConfig

        with pytest.raises(ValueError, match="ssl_mode must be one of"):
            MariaDBSSLConfig(ssl_mode="invalid_mode")

    def test_ssl_config_with_all_paths(self):
        """Test SSL config with all certificate paths."""
        from ddcdatabases import MariaDBSSLConfig

        config = MariaDBSSLConfig(
            ssl_mode="VERIFY_IDENTITY",
            ssl_ca_cert_path="/path/to/ca.pem",
            ssl_client_cert_path="/path/to/client.pem",
            ssl_client_key_path="/path/to/client-key.pem",
        )
        assert config.ssl_mode == "VERIFY_IDENTITY"
        assert config.ssl_ca_cert_path == "/path/to/ca.pem"
        assert config.ssl_client_cert_path == "/path/to/client.pem"
        assert config.ssl_client_key_path == "/path/to/client-key.pem"

    def test_ssl_config_immutability(self):
        """Test that SSL config is immutable (frozen)."""
        from ddcdatabases import MariaDBSSLConfig

        config = MariaDBSSLConfig(ssl_mode="REQUIRED")
        with pytest.raises(AttributeError):
            config.ssl_mode = "DISABLED"  # noqa

    def test_default_values_are_none(self):
        """Test MariaDB SSL config default values are None."""
        from ddcdatabases import MariaDBSSLConfig

        config = MariaDBSSLConfig()
        assert config.ssl_mode is None
        assert config.ssl_ca_cert_path is None
        assert config.ssl_client_cert_path is None
        assert config.ssl_client_key_path is None
