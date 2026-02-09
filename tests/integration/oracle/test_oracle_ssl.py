"""Integration tests for Oracle SSL configuration."""

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.integration


class TestOracleSSLIntegration:
    """Integration tests for Oracle SSL configuration."""

    def test_connection_with_ssl_disabled(self, oracle_container):
        """Test Oracle connection with SSL disabled."""
        from ddcDatabases import Oracle
        from ddcDatabases.oracle import OracleSSLConfig

        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        with Oracle(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
            ssl_config=OracleSSLConfig(ssl_enabled=False),
        ) as session:
            result = session.execute(sa.text("SELECT 1 FROM dual")).scalar()
            assert result == 1

    def test_ssl_config_is_accessible(self, oracle_container):
        """Test that Oracle SSL config is accessible."""
        from ddcDatabases import Oracle
        from ddcDatabases.oracle import OracleSSLConfig

        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        oracle = Oracle(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
            ssl_config=OracleSSLConfig(ssl_enabled=False, ssl_wallet_path="/path/to/wallet"),
        )

        ssl_info = oracle.get_ssl_info()
        assert ssl_info.ssl_enabled is False
        assert ssl_info.ssl_wallet_path == "/path/to/wallet"

    def test_ssl_config_immutable(self, oracle_container):
        """Test that Oracle SSL config is immutable."""
        from ddcDatabases import Oracle
        from ddcDatabases.oracle import OracleSSLConfig

        port = oracle_container.get_exposed_port(1521)
        host = oracle_container.get_container_host_ip()

        oracle = Oracle(
            host=host,
            port=int(port),
            user="system",
            password=oracle_container.oracle_password,
            servicename="FREEPDB1",
            ssl_config=OracleSSLConfig(ssl_enabled=False),
        )

        ssl_info = oracle.get_ssl_info()
        with pytest.raises(AttributeError):
            ssl_info.ssl_enabled = True  # noqa
