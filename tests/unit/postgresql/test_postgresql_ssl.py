"""Tests for PostgreSQL SSL configuration."""

import pytest


class TestPostgreSQLSSLConfig:
    """Test PostgreSQL SSL configuration validation."""

    def test_valid_ssl_modes(self):
        """Test all valid PostgreSQL SSL modes."""
        from ddcDatabases.core.constants import POSTGRESQL_SSL_MODES
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        for mode in POSTGRESQL_SSL_MODES:
            config = PostgreSQLSSLConfig(ssl_mode=mode)
            assert config.ssl_mode == mode

    def test_invalid_ssl_mode_raises_error(self):
        """Test that invalid SSL mode raises ValueError."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        with pytest.raises(ValueError, match="ssl_mode must be one of"):
            PostgreSQLSSLConfig(ssl_mode="invalid_mode")

    def test_ssl_config_with_all_paths(self):
        """Test SSL config with all certificate paths."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        config = PostgreSQLSSLConfig(
            ssl_mode="verify-full",
            ssl_ca_cert_path="/path/to/ca.pem",
            ssl_client_cert_path="/path/to/client.pem",
            ssl_client_key_path="/path/to/client-key.pem",
        )
        assert config.ssl_mode == "verify-full"
        assert config.ssl_ca_cert_path == "/path/to/ca.pem"
        assert config.ssl_client_cert_path == "/path/to/client.pem"
        assert config.ssl_client_key_path == "/path/to/client-key.pem"

    def test_ssl_config_immutability(self):
        """Test that SSL config is immutable (frozen)."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        config = PostgreSQLSSLConfig(ssl_mode="require")
        with pytest.raises(AttributeError):
            config.ssl_mode = "disable"  # noqa

    def test_ssl_mode_none_by_default(self):
        """Test default SSL mode is None (not set)."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        config = PostgreSQLSSLConfig()
        assert config.ssl_mode is None

    def test_ssl_modes_case_insensitive_validation(self):
        """Test that PostgreSQL SSL mode validation is case-insensitive."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        config = PostgreSQLSSLConfig(ssl_mode="REQUIRE")
        assert config.ssl_mode == "REQUIRE"

    def test_verify_ca_mode(self):
        """Test verify-ca SSL mode."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        config = PostgreSQLSSLConfig(
            ssl_mode="verify-ca",
            ssl_ca_cert_path="/path/to/ca.pem",
        )
        assert config.ssl_mode == "verify-ca"

    def test_all_ssl_modes_are_valid(self):
        """Test that all documented SSL modes are accepted."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        valid_modes = ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]
        for mode in valid_modes:
            config = PostgreSQLSSLConfig(ssl_mode=mode)
            assert config.ssl_mode == mode

    def test_ssl_ca_cert_path_only(self):
        """Test SSL config with only CA certificate path."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        config = PostgreSQLSSLConfig(
            ssl_mode="verify-ca",
            ssl_ca_cert_path="/path/to/ca.pem",
        )
        assert config.ssl_ca_cert_path == "/path/to/ca.pem"
        assert config.ssl_client_cert_path is None
        assert config.ssl_client_key_path is None

    def test_ssl_client_cert_without_key(self):
        """Test SSL config with client cert but no key."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        config = PostgreSQLSSLConfig(
            ssl_mode="verify-full",
            ssl_client_cert_path="/path/to/client.pem",
        )
        assert config.ssl_client_cert_path == "/path/to/client.pem"
        assert config.ssl_client_key_path is None
