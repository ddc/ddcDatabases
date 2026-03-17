"""Tests for MongoDB TLS configuration."""

import pytest


class TestMongoDBTLSConfig:
    """Test MongoDB TLS configuration validation."""

    def test_default_values_are_none(self):
        """Test MongoDB TLS config default values are None."""
        from ddcdatabases.mongodb import MongoDBTLSConfig

        config = MongoDBTLSConfig()
        assert config.tls_enabled is None
        assert config.tls_ca_cert_path is None
        assert config.tls_cert_key_path is None
        assert config.tls_allow_invalid_certificates is None

    def test_tls_enabled(self):
        """Test MongoDB TLS enabled configuration."""
        from ddcdatabases.mongodb import MongoDBTLSConfig

        config = MongoDBTLSConfig(tls_enabled=True)
        assert config.tls_enabled is True

    def test_tls_config_with_all_paths(self):
        """Test MongoDB TLS config with all certificate paths."""
        from ddcdatabases.mongodb import MongoDBTLSConfig

        config = MongoDBTLSConfig(
            tls_enabled=True,
            tls_ca_cert_path="/path/to/ca.pem",
            tls_cert_key_path="/path/to/cert.pem",
            tls_allow_invalid_certificates=True,
        )
        assert config.tls_enabled is True
        assert config.tls_ca_cert_path == "/path/to/ca.pem"
        assert config.tls_cert_key_path == "/path/to/cert.pem"
        assert config.tls_allow_invalid_certificates is True

    def test_tls_config_immutability(self):
        """Test that TLS config is immutable (frozen)."""
        from ddcdatabases.mongodb import MongoDBTLSConfig

        config = MongoDBTLSConfig(tls_enabled=True)
        with pytest.raises(AttributeError):
            config.tls_enabled = False  # noqa

    def test_tls_disabled(self):
        """Test MongoDB TLS disabled configuration."""
        from ddcdatabases.mongodb import MongoDBTLSConfig

        config = MongoDBTLSConfig(tls_enabled=False)
        assert config.tls_enabled is False

    def test_tls_allow_invalid_certificates(self):
        """Test MongoDB TLS allow invalid certificates."""
        from ddcdatabases.mongodb import MongoDBTLSConfig

        config = MongoDBTLSConfig(
            tls_enabled=True,
            tls_allow_invalid_certificates=True,
        )
        assert config.tls_allow_invalid_certificates is True

    def test_tls_ca_cert_path_only(self):
        """Test MongoDB TLS config with only CA cert path."""
        from ddcdatabases.mongodb import MongoDBTLSConfig

        config = MongoDBTLSConfig(
            tls_enabled=True,
            tls_ca_cert_path="/path/to/ca.pem",
        )
        assert config.tls_ca_cert_path == "/path/to/ca.pem"
        assert config.tls_cert_key_path is None

    def test_tls_cert_key_path_only(self):
        """Test MongoDB TLS config with only cert key path."""
        from ddcdatabases.mongodb import MongoDBTLSConfig

        config = MongoDBTLSConfig(
            tls_enabled=True,
            tls_cert_key_path="/path/to/cert.pem",
        )
        assert config.tls_cert_key_path == "/path/to/cert.pem"
        assert config.tls_ca_cert_path is None
