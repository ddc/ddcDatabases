"""Tests for SSL/TLS constants and shared SSL functionality."""


class TestSSLConstantsIntegrity:
    """Test SSL constants integrity across the module."""

    def test_postgresql_ssl_modes_frozenset(self):
        """Test that PostgreSQL SSL modes is a frozenset."""
        from ddcDatabases.core.constants import POSTGRESQL_SSL_MODES

        assert isinstance(POSTGRESQL_SSL_MODES, frozenset)
        assert len(POSTGRESQL_SSL_MODES) == 6

    def test_mysql_ssl_modes_frozenset(self):
        """Test that MySQL SSL modes is a frozenset."""
        from ddcDatabases.core.constants import MYSQL_SSL_MODES

        assert isinstance(MYSQL_SSL_MODES, frozenset)
        assert len(MYSQL_SSL_MODES) == 5

    def test_postgresql_modes_content(self):
        """Test PostgreSQL SSL modes contain expected values."""
        from ddcDatabases.core.constants import POSTGRESQL_SSL_MODES

        expected_modes = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
        assert POSTGRESQL_SSL_MODES == expected_modes

    def test_mysql_modes_content(self):
        """Test MySQL SSL modes contain expected values."""
        from ddcDatabases.core.constants import MYSQL_SSL_MODES

        expected_modes = {"DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"}
        assert MYSQL_SSL_MODES == expected_modes
