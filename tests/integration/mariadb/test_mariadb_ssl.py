"""Integration tests for MariaDB SSL configuration.

Note: MariaDB uses MySQL driver and SSL configuration, these tests verify
the MariaDB aliases work correctly.
"""

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.integration


class TestMariaDBSSLIntegration:
    """Integration tests for MariaDB SSL configuration."""

    def test_connection_with_ssl_disabled(self, mariadb_container):
        """Test MariaDB connection with SSL explicitly disabled."""
        from ddcdatabases import MariaDB, MariaDBSSLConfig

        port = mariadb_container.get_exposed_port(3306)
        host = mariadb_container.get_container_host_ip()

        with MariaDB(
            host=host,
            port=int(port),
            user=mariadb_container.username,
            password=mariadb_container.password,
            database=mariadb_container.dbname,
            ssl_config=MariaDBSSLConfig(ssl_mode="DISABLED"),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1

    def test_connection_with_ssl_preferred(self, mariadb_container):
        """Test MariaDB connection with SSL preferred mode."""
        from ddcdatabases import MariaDB, MariaDBSSLConfig

        port = mariadb_container.get_exposed_port(3306)
        host = mariadb_container.get_container_host_ip()

        with MariaDB(
            host=host,
            port=int(port),
            user=mariadb_container.username,
            password=mariadb_container.password,
            database=mariadb_container.dbname,
            ssl_config=MariaDBSSLConfig(ssl_mode="PREFERRED"),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1

    def test_ssl_config_is_accessible(self, mariadb_container):
        """Test that MariaDB SSL config is accessible."""
        from ddcdatabases import MariaDB, MariaDBSSLConfig

        port = mariadb_container.get_exposed_port(3306)
        host = mariadb_container.get_container_host_ip()

        mariadb = MariaDB(
            host=host,
            port=int(port),
            user=mariadb_container.username,
            password=mariadb_container.password,
            database=mariadb_container.dbname,
            ssl_config=MariaDBSSLConfig(
                ssl_mode="PREFERRED",
                ssl_ca_cert_path="/path/to/ca.pem",
            ),
        )

        ssl_info = mariadb.get_ssl_info()
        assert ssl_info.ssl_mode == "PREFERRED"
        assert ssl_info.ssl_ca_cert_path == "/path/to/ca.pem"

    def test_ssl_config_immutable(self, mariadb_container):
        """Test that MariaDB SSL config is immutable."""
        from ddcdatabases import MariaDB, MariaDBSSLConfig

        port = mariadb_container.get_exposed_port(3306)
        host = mariadb_container.get_container_host_ip()

        mariadb = MariaDB(
            host=host,
            port=int(port),
            user=mariadb_container.username,
            password=mariadb_container.password,
            database=mariadb_container.dbname,
            ssl_config=MariaDBSSLConfig(ssl_mode="DISABLED"),
        )

        ssl_info = mariadb.get_ssl_info()
        with pytest.raises(AttributeError):
            ssl_info.ssl_mode = "REQUIRED"  # noqa
