"""Integration tests for PostgreSQL SSL configuration."""

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.integration


class TestPostgreSQLSSLIntegration:
    """Integration tests for PostgreSQL SSL configuration."""

    def test_connection_with_ssl_disabled(self, postgres_container):
        """Test PostgreSQL connection with SSL explicitly disabled."""
        from ddcDatabases import PostgreSQL
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            ssl_config=PostgreSQLSSLConfig(ssl_mode="disable"),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1

    def test_connection_with_ssl_prefer(self, postgres_container):
        """Test PostgreSQL connection with SSL prefer mode (falls back to no SSL)."""
        from ddcDatabases import PostgreSQL
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            ssl_config=PostgreSQLSSLConfig(ssl_mode="prefer"),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1

    def test_connection_with_ssl_allow(self, postgres_container):
        """Test PostgreSQL connection with SSL allow mode."""
        from ddcDatabases import PostgreSQL
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        with PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            ssl_config=PostgreSQLSSLConfig(ssl_mode="allow"),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1

    def test_ssl_config_is_accessible(self, postgres_container):
        """Test that SSL config is accessible through getter method."""
        from ddcDatabases import PostgreSQL
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        pg = PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            ssl_config=PostgreSQLSSLConfig(
                ssl_mode="prefer",
                ssl_ca_cert_path="/path/to/ca.pem",
            ),
        )

        ssl_info = pg.get_ssl_info()
        assert ssl_info.ssl_mode == "prefer"
        assert ssl_info.ssl_ca_cert_path == "/path/to/ca.pem"

    def test_ssl_config_immutable(self, postgres_container):
        """Test that SSL config is immutable."""
        from ddcDatabases import PostgreSQL
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        port = postgres_container.get_exposed_port(5432)
        host = postgres_container.get_container_host_ip()

        pg = PostgreSQL(
            host=host,
            port=int(port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            ssl_config=PostgreSQLSSLConfig(ssl_mode="disable"),
        )

        ssl_info = pg.get_ssl_info()
        with pytest.raises(AttributeError):
            ssl_info.ssl_mode = "require"  # noqa

    def test_invalid_ssl_mode(self):
        """Test PostgreSQL rejects invalid SSL mode."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        with pytest.raises(ValueError, match="ssl_mode must be one of"):
            PostgreSQLSSLConfig(ssl_mode="invalid_mode")

    def test_valid_ssl_modes(self):
        """Test PostgreSQL accepts all valid SSL modes."""
        from ddcDatabases.postgresql import PostgreSQLSSLConfig

        valid_modes = ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]
        for mode in valid_modes:
            config = PostgreSQLSSLConfig(ssl_mode=mode)
            assert config.ssl_mode == mode
