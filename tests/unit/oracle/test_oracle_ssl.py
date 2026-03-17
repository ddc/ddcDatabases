"""Tests for Oracle SSL configuration."""

import pytest


class TestOracleSSLConfig:
    """Test Oracle SSL configuration validation."""

    def test_default_values_are_none(self):
        """Test Oracle SSL config default values are None."""
        from ddcdatabases.oracle import OracleSSLConfig

        config = OracleSSLConfig()
        assert config.ssl_enabled is None
        assert config.ssl_wallet_path is None

    def test_ssl_enabled_with_wallet(self):
        """Test Oracle SSL enabled with wallet path."""
        from ddcdatabases.oracle import OracleSSLConfig

        config = OracleSSLConfig(
            ssl_enabled=True,
            ssl_wallet_path="/path/to/wallet",
        )
        assert config.ssl_enabled is True
        assert config.ssl_wallet_path == "/path/to/wallet"

    def test_ssl_config_immutability(self):
        """Test that SSL config is immutable (frozen)."""
        from ddcdatabases.oracle import OracleSSLConfig

        config = OracleSSLConfig(ssl_enabled=True)
        with pytest.raises(AttributeError):
            config.ssl_enabled = False  # noqa

    def test_ssl_disabled(self):
        """Test Oracle SSL disabled configuration."""
        from ddcdatabases.oracle import OracleSSLConfig

        config = OracleSSLConfig(ssl_enabled=False)
        assert config.ssl_enabled is False

    def test_ssl_enabled_without_wallet(self):
        """Test Oracle SSL enabled without wallet path."""
        from ddcdatabases.oracle import OracleSSLConfig

        config = OracleSSLConfig(ssl_enabled=True)
        assert config.ssl_enabled is True
        assert config.ssl_wallet_path is None

    def test_wallet_path_only(self):
        """Test Oracle SSL config with only wallet path."""
        from ddcdatabases.oracle import OracleSSLConfig

        config = OracleSSLConfig(ssl_wallet_path="/path/to/wallet")
        assert config.ssl_wallet_path == "/path/to/wallet"
        assert config.ssl_enabled is None
