"""Integration tests for close_all_persistent_connections with multiple database types."""

import pytest
import sqlalchemy as sa

# noinspection PyProtectedMember
from ddcDatabases.core.persistent import (
    MySQLPersistent,
    PostgreSQLPersistent,
    close_all_persistent_connections,
)

pytestmark = pytest.mark.integration


class TestCloseAllPersistentConnectionsIntegration:
    """Integration tests for close_all_persistent_connections."""

    def setup_method(self):
        """Clear persistent connections before each test."""
        close_all_persistent_connections()

    def teardown_method(self):
        """Clean up after each test."""
        close_all_persistent_connections()

    def test_close_all_with_active_connections(self, postgres_container, mysql_container):
        """Test close_all properly closes active connections."""
        pg_port = postgres_container.get_exposed_port(5432)
        pg_host = postgres_container.get_container_host_ip()
        mysql_port = mysql_container.get_exposed_port(3306)
        mysql_host = mysql_container.get_container_host_ip()

        pg_conn = PostgreSQLPersistent(
            host=pg_host,
            port=int(pg_port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            async_mode=False,
        )
        mysql_conn = MySQLPersistent(
            host=mysql_host,
            port=int(mysql_port),
            user=mysql_container.username,
            password=mysql_container.password,
            database=mysql_container.dbname,
            async_mode=False,
        )

        # Establish connections
        with pg_conn as pg_session:
            pg_session.execute(sa.text("SELECT 1"))
        with mysql_conn as mysql_session:
            mysql_session.execute(sa.text("SELECT 1"))

        # Close all
        close_all_persistent_connections()

        # Connections should no longer be active
        # Getting new instances should create fresh connections
        pg_conn_new = PostgreSQLPersistent(
            host=pg_host,
            port=int(pg_port),
            user=postgres_container.username,
            password=postgres_container.password,
            database=postgres_container.dbname,
            async_mode=False,
        )

        # Should be a new instance after close_all
        assert not pg_conn_new.is_connected

        pg_conn_new.shutdown()
