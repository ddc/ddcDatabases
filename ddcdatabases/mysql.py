import logging
from .core.base import BaseConnection
from .core.configs import (
    CONNECTION_RETRY_FIELD_MAP,
    OPERATION_RETRY_FIELD_MAP,
    BaseConnectionConfig,
    BaseOperationRetryConfig,
    BasePoolConfig,
    BaseRetryConfig,
    BaseSessionConfig,
    BaseSSLConfig,
    merge_config_with_settings,
)
from .core.constants import MYSQL_SSL_MODES
from .core.settings import get_mysql_settings
from dataclasses import dataclass
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True, slots=True)
class MySQLConnectionConfig(BaseConnectionConfig):
    database: str | None = None


@dataclass(frozen=True, slots=True)
class MySQLSSLConfig(BaseSSLConfig):
    def __post_init__(self) -> None:
        if self.ssl_mode is not None and self.ssl_mode.upper() not in MYSQL_SSL_MODES:
            raise ValueError(f"ssl_mode must be one of {sorted(MYSQL_SSL_MODES)}, got '{self.ssl_mode}'")


@dataclass(frozen=True, slots=True)
class MySQLPoolConfig(BasePoolConfig):
    pass


@dataclass(frozen=True, slots=True)
class MySQLSessionConfig(BaseSessionConfig):
    pass


@dataclass(frozen=True, slots=True)
class MySQLConnectionRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class MySQLOperationRetryConfig(BaseOperationRetryConfig):
    pass


class MySQL(BaseConnection):
    """
    Class to handle MySQL connections.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        pool_config: MySQLPoolConfig | None = None,
        session_config: MySQLSessionConfig | None = None,
        connection_retry_config: MySQLConnectionRetryConfig | None = None,
        operation_retry_config: MySQLOperationRetryConfig | None = None,
        ssl_config: MySQLSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: Any = None,
    ) -> None:
        _settings = get_mysql_settings()

        # Normalize localhost to 127.0.0.1 to force TCP connection
        # mysqlclient uses Unix socket when host is 'localhost'
        _host = host or _settings.host
        if _host and _host.lower() == "localhost":
            _host = "127.0.0.1"

        self._connection_config = MySQLConnectionConfig(
            host=_host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
        )

        self._pool_config = merge_config_with_settings(MySQLPoolConfig, pool_config, _settings)
        self._session_config = merge_config_with_settings(MySQLSessionConfig, session_config, _settings)

        _ssl = ssl_config or MySQLSSLConfig()
        self._ssl_config = MySQLSSLConfig(
            ssl_mode=_ssl.ssl_mode if _ssl.ssl_mode is not None else _settings.ssl_mode,
            ssl_ca_cert_path=_ssl.ssl_ca_cert_path if _ssl.ssl_ca_cert_path is not None else _settings.ssl_ca_cert_path,
            ssl_client_cert_path=(
                _ssl.ssl_client_cert_path if _ssl.ssl_client_cert_path is not None else _settings.ssl_client_cert_path
            ),
            ssl_client_key_path=(
                _ssl.ssl_client_key_path if _ssl.ssl_client_key_path is not None else _settings.ssl_client_key_path
            ),
        )

        self.sync_driver = _settings.sync_driver
        self.async_driver = _settings.async_driver

        self.connection_url = {
            "host": self._connection_config.host,
            "port": self._connection_config.port,
            "username": self._connection_config.user,
            "password": self._connection_config.password,
            "database": self._connection_config.database,
        }

        self.extra_engine_args = extra_engine_args or {}
        _connect_args: dict[str, Any] = {
            "charset": "utf8mb4",
            "autocommit": self._session_config.autocommit,
            "connect_timeout": self._pool_config.connection_timeout,
        }
        if self._ssl_config.ssl_mode and self._ssl_config.ssl_mode != "DISABLED":
            ssl_dict = {}
            if self._ssl_config.ssl_ca_cert_path:
                ssl_dict["ca"] = self._ssl_config.ssl_ca_cert_path
            if self._ssl_config.ssl_client_cert_path:
                ssl_dict["cert"] = self._ssl_config.ssl_client_cert_path
            if self._ssl_config.ssl_client_key_path:
                ssl_dict["key"] = self._ssl_config.ssl_client_key_path
            _connect_args["ssl"] = ssl_dict
        self.engine_args = {
            "echo": self._session_config.echo,
            "pool_pre_ping": True,
            "pool_recycle": self._pool_config.pool_recycle,
            "pool_size": self._pool_config.pool_size,
            "max_overflow": self._pool_config.max_overflow,
            "connect_args": _connect_args,
            **self.extra_engine_args,
        }

        self._connection_retry_config = merge_config_with_settings(
            MySQLConnectionRetryConfig, connection_retry_config, _settings, CONNECTION_RETRY_FIELD_MAP
        )
        self._operation_retry_config = merge_config_with_settings(
            MySQLOperationRetryConfig, operation_retry_config, _settings, OPERATION_RETRY_FIELD_MAP
        )

        self.logger = logger if logger is not None else _logger

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self._session_config.autoflush,
            expire_on_commit=self._session_config.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
            connection_retry_config=self._connection_retry_config,
            operation_retry_config=self._operation_retry_config,
            logger=self.logger,
        )

        self.logger.debug(
            f"Initializing MySQL(host={self._connection_config.host}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database})"
        )

    def __repr__(self) -> str:
        return (
            "MySQL("
            f"host={self._connection_config.host!r}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database!r}, "
            f"pool_size={self._pool_config.pool_size}, "
            f"echo={self._session_config.echo}"
            ")"
        )

    def get_connection_info(self) -> MySQLConnectionConfig:
        return self._connection_config

    def get_pool_info(self) -> MySQLPoolConfig:
        return self._pool_config

    def get_session_info(self) -> MySQLSessionConfig:
        return self._session_config

    def get_connection_retry_info(self) -> MySQLConnectionRetryConfig:
        return self._connection_retry_config

    def get_operation_retry_info(self) -> MySQLOperationRetryConfig:
        return self._operation_retry_config

    def get_ssl_info(self) -> MySQLSSLConfig:
        return self._ssl_config
