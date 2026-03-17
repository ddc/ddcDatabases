"""Integration tests for MySQL SSL configuration."""

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.integration


class TestMySQLSSLIntegration:
    """Integration tests for MySQL SSL configuration."""

    def test_connection_with_ssl_disabled(self, mysql_container):
        """Test MySQL connection with SSL explicitly disabled."""
        from ddcdatabases import MySQL
        from ddcdatabases.mysql import MySQLSSLConfig

        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        with MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            ssl_config=MySQLSSLConfig(ssl_mode="DISABLED"),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1

    def test_connection_with_ssl_preferred(self, mysql_container):
        """Test MySQL connection with SSL preferred mode."""
        from ddcdatabases import MySQL
        from ddcdatabases.mysql import MySQLSSLConfig

        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        with MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            ssl_config=MySQLSSLConfig(ssl_mode="PREFERRED"),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1

    def test_ssl_config_is_accessible(self, mysql_container):
        """Test that MySQL SSL config is accessible."""
        from ddcdatabases import MySQL
        from ddcdatabases.mysql import MySQLSSLConfig

        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        mysql = MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            ssl_config=MySQLSSLConfig(
                ssl_mode="PREFERRED",
                ssl_ca_cert_path="/path/to/ca.pem",
            ),
        )

        ssl_info = mysql.get_ssl_info()
        assert ssl_info.ssl_mode == "PREFERRED"
        assert ssl_info.ssl_ca_cert_path == "/path/to/ca.pem"

    def test_ssl_config_immutable(self, mysql_container):
        """Test that MySQL SSL config is immutable."""
        from ddcdatabases import MySQL
        from ddcdatabases.mysql import MySQLSSLConfig

        port = mysql_container.get_exposed_port(3306)
        host = mysql_container.get_container_host_ip()

        mysql = MySQL(
            host=host,
            port=int(port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            ssl_config=MySQLSSLConfig(ssl_mode="DISABLED"),
        )

        ssl_info = mysql.get_ssl_info()
        with pytest.raises(AttributeError):
            ssl_info.ssl_mode = "REQUIRED"  # noqa

    def test_invalid_ssl_mode(self):
        """Test MySQL rejects invalid SSL mode."""
        from ddcdatabases.mysql import MySQLSSLConfig

        with pytest.raises(ValueError, match="ssl_mode must be one of"):
            MySQLSSLConfig(ssl_mode="invalid_mode")

    def test_valid_ssl_modes(self):
        """Test MySQL accepts all valid SSL modes."""
        from ddcdatabases.mysql import MySQLSSLConfig

        valid_modes = ["DISABLED", "PREFERRED", "REQUIRED", "VERIFY_CA", "VERIFY_IDENTITY"]
        for mode in valid_modes:
            config = MySQLSSLConfig(ssl_mode=mode)
            assert config.ssl_mode == mode
