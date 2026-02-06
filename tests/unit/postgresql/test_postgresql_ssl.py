"""Tests for PostgreSQL SSL configuration."""

import os
import pytest
import ssl
from importlib.util import find_spec
from unittest.mock import AsyncMock, MagicMock, patch

POSTGRESQL_AVAILABLE = find_spec("asyncpg") is not None and find_spec("psycopg") is not None

pytestmark = pytest.mark.skipif(not POSTGRESQL_AVAILABLE, reason="PostgreSQL drivers not available")


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


class TestPostgreSQLSSLEngine:
    """Test that SSL cert paths are correctly passed to sync/async engines."""

    # noinspection PyMethodMayBeStatic
    def setup_method(self):
        """Clear cache before each test."""
        from ddcDatabases.core.settings import get_postgresql_settings

        get_postgresql_settings.cache_clear()

        import ddcDatabases.core.settings

        ddcDatabases.core.settings._dotenv_loaded = False

    @patch("ddcDatabases.postgresql.get_postgresql_settings")
    def test_async_engine_ssl_with_cert_paths_uses_ssl_context(self, mock_get_settings):
        """Test that _get_async_engine passes an ssl.SSLContext when cert paths are provided."""
        from ddcDatabases.postgresql import PostgreSQL, PostgreSQLSSLConfig

        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.database = "postgres"
        mock_settings.schema = "public"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_settings.ssl_mode = "verify-full"
        mock_settings.ssl_ca_cert_path = "/path/to/ca.pem"
        mock_settings.ssl_client_cert_path = "/path/to/client.pem"
        mock_settings.ssl_client_key_path = "/path/to/client-key.pem"
        mock_settings.connection_enable_retry = None
        mock_settings.connection_max_retries = None
        mock_settings.connection_initial_retry_delay = None
        mock_settings.connection_max_retry_delay = None
        mock_settings.operation_enable_retry = None
        mock_settings.operation_max_retries = None
        mock_settings.operation_initial_retry_delay = None
        mock_settings.operation_max_retry_delay = None
        mock_settings.operation_jitter = None
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(
            ssl_config=PostgreSQLSSLConfig(
                ssl_mode="verify-full",
                ssl_ca_cert_path="/path/to/ca.pem",
                ssl_client_cert_path="/path/to/client.pem",
                ssl_client_key_path="/path/to/client-key.pem",
            )
        )

        captured_args = {}
        mock_ssl_context = MagicMock(spec=ssl.SSLContext)

        with (
            patch("ddcDatabases.postgresql.create_async_engine") as mock_create,
            patch("ddcDatabases.postgresql._ssl_module.create_default_context", return_value=mock_ssl_context),
        ):
            mock_engine = MagicMock()
            mock_engine.dispose = AsyncMock()
            mock_create.return_value = mock_engine

            import asyncio

            async def _test():
                async with postgresql._get_async_engine() as engine:
                    captured_args.update(mock_create.call_args.kwargs)

            asyncio.run(_test())

        connect_args = captured_args.get("connect_args", {})
        assert "ssl" in connect_args, "ssl must be in async connect_args when cert paths are set"
        assert connect_args["ssl"] is mock_ssl_context
        assert mock_ssl_context.minimum_version == ssl.TLSVersion.TLSv1_3
        mock_ssl_context.load_cert_chain.assert_called_once_with(
            certfile="/path/to/client.pem",
            keyfile="/path/to/client-key.pem",
        )

    @patch("ddcDatabases.postgresql.get_postgresql_settings")
    def test_async_engine_ssl_without_cert_paths_uses_mode_string(self, mock_get_settings):
        """Test that _get_async_engine passes the mode string when no cert paths are provided."""
        from ddcDatabases.postgresql import PostgreSQL, PostgreSQLSSLConfig

        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.database = "postgres"
        mock_settings.schema = "public"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_settings.ssl_mode = "require"
        mock_settings.ssl_ca_cert_path = None
        mock_settings.ssl_client_cert_path = None
        mock_settings.ssl_client_key_path = None
        mock_settings.connection_enable_retry = None
        mock_settings.connection_max_retries = None
        mock_settings.connection_initial_retry_delay = None
        mock_settings.connection_max_retry_delay = None
        mock_settings.operation_enable_retry = None
        mock_settings.operation_max_retries = None
        mock_settings.operation_initial_retry_delay = None
        mock_settings.operation_max_retry_delay = None
        mock_settings.operation_jitter = None
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(ssl_config=PostgreSQLSSLConfig(ssl_mode="require"))

        captured_args = {}

        with patch("ddcDatabases.postgresql.create_async_engine") as mock_create:
            mock_engine = MagicMock()
            mock_engine.dispose = AsyncMock()
            mock_create.return_value = mock_engine

            import asyncio

            async def _test():
                async with postgresql._get_async_engine() as engine:
                    captured_args.update(mock_create.call_args.kwargs)

            asyncio.run(_test())

        connect_args = captured_args.get("connect_args", {})
        assert connect_args.get("ssl") == "require", f"Expected mode string 'require', got {connect_args.get('ssl')!r}"

    @patch("ddcDatabases.postgresql.get_postgresql_settings")
    def test_async_engine_ssl_ca_only_no_client_certs(self, mock_get_settings):
        """Test that _get_async_engine creates SSLContext with CA cert only (no client cert/key)."""
        from ddcDatabases.postgresql import PostgreSQL, PostgreSQLSSLConfig

        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.database = "postgres"
        mock_settings.schema = "public"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_settings.ssl_mode = "verify-ca"
        mock_settings.ssl_ca_cert_path = "/path/to/ca.pem"
        mock_settings.ssl_client_cert_path = None
        mock_settings.ssl_client_key_path = None
        mock_settings.connection_enable_retry = None
        mock_settings.connection_max_retries = None
        mock_settings.connection_initial_retry_delay = None
        mock_settings.connection_max_retry_delay = None
        mock_settings.operation_enable_retry = None
        mock_settings.operation_max_retries = None
        mock_settings.operation_initial_retry_delay = None
        mock_settings.operation_max_retry_delay = None
        mock_settings.operation_jitter = None
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(
            ssl_config=PostgreSQLSSLConfig(
                ssl_mode="verify-ca",
                ssl_ca_cert_path="/path/to/ca.pem",
            )
        )

        captured_args = {}
        mock_ssl_context = MagicMock(spec=ssl.SSLContext)

        with (
            patch("ddcDatabases.postgresql.create_async_engine") as mock_create,
            patch("ddcDatabases.postgresql._ssl_module.create_default_context", return_value=mock_ssl_context),
        ):
            mock_engine = MagicMock()
            mock_engine.dispose = AsyncMock()
            mock_create.return_value = mock_engine

            import asyncio

            async def _test():
                async with postgresql._get_async_engine() as engine:
                    captured_args.update(mock_create.call_args.kwargs)

            asyncio.run(_test())

        connect_args = captured_args.get("connect_args", {})
        assert connect_args["ssl"] is mock_ssl_context
        assert mock_ssl_context.minimum_version == ssl.TLSVersion.TLSv1_3
        mock_ssl_context.load_cert_chain.assert_not_called()

    @patch("ddcDatabases.postgresql.get_postgresql_settings")
    def test_sync_engine_ssl_with_cert_paths(self, mock_get_settings):
        """Test that _get_engine passes SSL cert paths as connect_args for psycopg."""
        from ddcDatabases.postgresql import PostgreSQL, PostgreSQLSSLConfig

        mock_settings = MagicMock()
        mock_settings.host = "localhost"
        mock_settings.port = 5432
        mock_settings.user = "postgres"
        mock_settings.password = "password"
        mock_settings.database = "postgres"
        mock_settings.schema = "public"
        mock_settings.echo = False
        mock_settings.autoflush = False
        mock_settings.expire_on_commit = False
        mock_settings.autocommit = False
        mock_settings.connection_timeout = 30
        mock_settings.pool_recycle = 3600
        mock_settings.pool_size = 25
        mock_settings.max_overflow = 50
        mock_settings.sync_driver = "postgresql+psycopg"
        mock_settings.async_driver = "postgresql+asyncpg"
        mock_settings.ssl_mode = "verify-full"
        mock_settings.ssl_ca_cert_path = "/path/to/ca.pem"
        mock_settings.ssl_client_cert_path = "/path/to/client.pem"
        mock_settings.ssl_client_key_path = "/path/to/client-key.pem"
        mock_settings.connection_enable_retry = None
        mock_settings.connection_max_retries = None
        mock_settings.connection_initial_retry_delay = None
        mock_settings.connection_max_retry_delay = None
        mock_settings.operation_enable_retry = None
        mock_settings.operation_max_retries = None
        mock_settings.operation_initial_retry_delay = None
        mock_settings.operation_max_retry_delay = None
        mock_settings.operation_jitter = None
        mock_get_settings.return_value = mock_settings

        postgresql = PostgreSQL(
            ssl_config=PostgreSQLSSLConfig(
                ssl_mode="verify-full",
                ssl_ca_cert_path="/path/to/ca.pem",
                ssl_client_cert_path="/path/to/client.pem",
                ssl_client_key_path="/path/to/client-key.pem",
            )
        )

        with patch("ddcDatabases.postgresql.create_engine") as mock_create:
            mock_engine = MagicMock()
            mock_create.return_value = mock_engine

            with postgresql._get_engine() as engine:
                captured_args = mock_create.call_args.kwargs

        connect_args = captured_args.get("connect_args", {})
        assert connect_args["sslmode"] == "verify-full"
        assert connect_args["sslrootcert"] == "/path/to/ca.pem"
        assert connect_args["sslcert"] == "/path/to/client.pem"
        assert connect_args["sslkey"] == "/path/to/client-key.pem"


class TestPostgreSQLSSLEnvVars:
    """Test that SSL settings are correctly read from environment variables."""

    # noinspection PyMethodMayBeStatic
    def setup_method(self):
        """Clear cache before each test."""
        from ddcDatabases.core.settings import get_postgresql_settings

        get_postgresql_settings.cache_clear()

        import ddcDatabases.core.settings

        ddcDatabases.core.settings._dotenv_loaded = False

    def test_ssl_cert_paths_from_env_vars(self):
        """Test that SSL cert paths are read from POSTGRESQL_SSL_* env vars."""
        from ddcDatabases.core.settings import PostgreSQLSettings

        with patch.dict(
            os.environ,
            {
                "POSTGRESQL_SSL_MODE": "verify-full",
                "POSTGRESQL_SSL_CA_CERT_PATH": "/env/path/to/ca.pem",
                "POSTGRESQL_SSL_CLIENT_CERT_PATH": "/env/path/to/client.pem",
                "POSTGRESQL_SSL_CLIENT_KEY_PATH": "/env/path/to/client-key.pem",
            },
        ):
            settings = PostgreSQLSettings()
            assert settings.ssl_mode == "verify-full"
            assert settings.ssl_ca_cert_path == "/env/path/to/ca.pem"
            assert settings.ssl_client_cert_path == "/env/path/to/client.pem"
            assert settings.ssl_client_key_path == "/env/path/to/client-key.pem"

    def test_ssl_env_vars_propagate_to_async_engine(self):
        """Test that SSL cert paths from env vars result in SSLContext for async engine."""
        from ddcDatabases.core.settings import PostgreSQLSettings
        from ddcDatabases.postgresql import PostgreSQL

        with patch.dict(
            os.environ,
            {
                "POSTGRESQL_SSL_MODE": "verify-full",
                "POSTGRESQL_SSL_CA_CERT_PATH": "/env/path/to/ca.pem",
                "POSTGRESQL_SSL_CLIENT_CERT_PATH": "/env/path/to/client.pem",
                "POSTGRESQL_SSL_CLIENT_KEY_PATH": "/env/path/to/client-key.pem",
            },
        ):
            mock_settings = PostgreSQLSettings()

        with patch("ddcDatabases.postgresql.get_postgresql_settings", return_value=mock_settings):
            postgresql = PostgreSQL()

        assert postgresql._ssl_config.ssl_ca_cert_path == "/env/path/to/ca.pem"
        assert postgresql._ssl_config.ssl_client_cert_path == "/env/path/to/client.pem"
        assert postgresql._ssl_config.ssl_client_key_path == "/env/path/to/client-key.pem"

        captured_args = {}
        mock_ssl_context = MagicMock(spec=ssl.SSLContext)

        with (
            patch("ddcDatabases.postgresql.create_async_engine") as mock_create,
            patch("ddcDatabases.postgresql._ssl_module.create_default_context", return_value=mock_ssl_context),
        ):
            mock_engine = MagicMock()
            mock_engine.dispose = AsyncMock()
            mock_create.return_value = mock_engine

            import asyncio

            async def _test():
                async with postgresql._get_async_engine() as engine:
                    captured_args.update(mock_create.call_args.kwargs)

            asyncio.run(_test())

        connect_args = captured_args.get("connect_args", {})
        assert connect_args["ssl"] is mock_ssl_context
        assert mock_ssl_context.minimum_version == ssl.TLSVersion.TLSv1_3
        mock_ssl_context.load_cert_chain.assert_called_once_with(
            certfile="/env/path/to/client.pem",
            keyfile="/env/path/to/client-key.pem",
        )
