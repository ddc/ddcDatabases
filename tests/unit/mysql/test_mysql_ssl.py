"""Tests for MySQL SSL configuration."""

import pytest


class TestMySQLSSLConfig:
    """Test MySQL SSL configuration validation."""

    def test_valid_ssl_modes(self):
        """Test all valid MySQL SSL modes."""
        from ddcDatabases.core.constants import MYSQL_SSL_MODES
        from ddcDatabases.mysql import MySQLSSLConfig

        for mode in MYSQL_SSL_MODES:
            config = MySQLSSLConfig(ssl_mode=mode)
            assert config.ssl_mode == mode

    def test_invalid_ssl_mode_raises_error(self):
        """Test that invalid SSL mode raises ValueError."""
        from ddcDatabases.mysql import MySQLSSLConfig

        with pytest.raises(ValueError, match="ssl_mode must be one of"):
            MySQLSSLConfig(ssl_mode="invalid_mode")

    def test_ssl_config_with_all_paths(self):
        """Test SSL config with all certificate paths."""
        from ddcDatabases.mysql import MySQLSSLConfig

        config = MySQLSSLConfig(
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
        from ddcDatabases.mysql import MySQLSSLConfig

        config = MySQLSSLConfig(ssl_mode="REQUIRED")
        with pytest.raises(AttributeError):
            config.ssl_mode = "DISABLED"  # noqa

    def test_ssl_mode_none_by_default(self):
        """Test default SSL mode is None (not set)."""
        from ddcDatabases.mysql import MySQLSSLConfig

        config = MySQLSSLConfig()
        assert config.ssl_mode is None

    def test_ssl_modes_case_insensitive_validation(self):
        """Test that MySQL SSL mode validation is case-insensitive."""
        from ddcDatabases.mysql import MySQLSSLConfig

        config = MySQLSSLConfig(ssl_mode="required")
        assert config.ssl_mode == "required"

    def test_preferred_mode(self):
        """Test PREFERRED SSL mode."""
        from ddcDatabases.mysql import MySQLSSLConfig

        config = MySQLSSLConfig(ssl_mode="PREFERRED")
        assert config.ssl_mode == "PREFERRED"

    def test_verify_ca_mode(self):
        """Test VERIFY_CA SSL mode."""
        from ddcDatabases.mysql import MySQLSSLConfig

        config = MySQLSSLConfig(
            ssl_mode="VERIFY_CA",
            ssl_ca_cert_path="/path/to/ca.pem",
        )
        assert config.ssl_mode == "VERIFY_CA"

    def test_all_ssl_modes_are_valid(self):
        """Test that all documented SSL modes are accepted."""
        from ddcDatabases.mysql import MySQLSSLConfig

        valid_modes = ["DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"]
        for mode in valid_modes:
            config = MySQLSSLConfig(ssl_mode=mode)
            assert config.ssl_mode == mode

    def test_ssl_ca_cert_path_only(self):
        """Test SSL config with only CA certificate path."""
        from ddcDatabases.mysql import MySQLSSLConfig

        config = MySQLSSLConfig(
            ssl_mode="VERIFY_CA",
            ssl_ca_cert_path="/path/to/ca.pem",
        )
        assert config.ssl_ca_cert_path == "/path/to/ca.pem"
        assert config.ssl_client_cert_path is None
        assert config.ssl_client_key_path is None
