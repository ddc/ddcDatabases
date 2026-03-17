"""Integration tests for MSSQL SSL configuration."""

import pytest
import sqlalchemy as sa

pytestmark = pytest.mark.integration


class TestMSSQLSSLIntegration:
    """Integration tests for MSSQL SSL configuration."""

    def test_connection_with_ssl_disabled(self, mssql_container):
        """Test MSSQL connection with SSL/encryption disabled."""
        from ddcdatabases import MSSQL
        from ddcdatabases.mssql import MSSQLSSLConfig

        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        with MSSQL(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="master",
            ssl_config=MSSQLSSLConfig(ssl_encrypt=False, ssl_trust_server_certificate=True),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1

    def test_ssl_config_is_accessible(self, mssql_container):
        """Test that MSSQL SSL config is accessible."""
        from ddcdatabases import MSSQL
        from ddcdatabases.mssql import MSSQLSSLConfig

        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        mssql = MSSQL(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="master",
            ssl_config=MSSQLSSLConfig(ssl_encrypt=True, ssl_trust_server_certificate=True),
        )

        ssl_info = mssql.get_ssl_info()
        assert ssl_info.ssl_encrypt is True
        assert ssl_info.ssl_trust_server_certificate is True

    def test_ssl_config_immutable(self, mssql_container):
        """Test that MSSQL SSL config is immutable."""
        from ddcdatabases import MSSQL
        from ddcdatabases.mssql import MSSQLSSLConfig

        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        mssql = MSSQL(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="master",
            ssl_config=MSSQLSSLConfig(ssl_encrypt=False),
        )

        ssl_info = mssql.get_ssl_info()
        with pytest.raises(AttributeError):
            ssl_info.ssl_encrypt = True  # noqa

    def test_connection_with_trust_certificate(self, mssql_container):
        """Test MSSQL connection with trust server certificate enabled."""
        from ddcdatabases import MSSQL
        from ddcdatabases.mssql import MSSQLSSLConfig

        port = mssql_container.get_exposed_port(1433)
        host = mssql_container.get_container_host_ip()

        with MSSQL(
            host=host,
            port=int(port),
            user="sa",
            password="Strong@Pass123",
            database="master",
            ssl_config=MSSQLSSLConfig(ssl_encrypt=True, ssl_trust_server_certificate=True),
        ) as session:
            result = session.execute(sa.text("SELECT 1")).scalar()
            assert result == 1
