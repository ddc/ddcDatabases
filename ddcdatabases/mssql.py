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
    merge_config_with_settings,
)
from .core.settings import get_mssql_settings
from dataclasses import dataclass
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True, slots=True)
class MSSQLConnectionConfig(BaseConnectionConfig):
    database: str | None = None
    schema: str | None = None
    odbcdriver_version: int | None = None


@dataclass(frozen=True, slots=True)
class MSSQLSSLConfig:
    ssl_encrypt: bool | None = None
    ssl_trust_server_certificate: bool | None = None
    ssl_ca_cert_path: str | None = None


@dataclass(frozen=True, slots=True)
class MSSQLPoolConfig(BasePoolConfig):
    pass


@dataclass(frozen=True, slots=True)
class MSSQLSessionConfig(BaseSessionConfig):
    pass


@dataclass(frozen=True, slots=True)
class MSSQLConnectionRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class MSSQLOperationRetryConfig(BaseOperationRetryConfig):
    pass


class MSSQL(BaseConnection):
    """
    Class to handle MSSQL connections.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        pool_config: MSSQLPoolConfig | None = None,
        session_config: MSSQLSessionConfig | None = None,
        connection_retry_config: MSSQLConnectionRetryConfig | None = None,
        operation_retry_config: MSSQLOperationRetryConfig | None = None,
        ssl_config: MSSQLSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: Any = None,
    ) -> None:
        _settings = get_mssql_settings()

        self._connection_config = MSSQLConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
            schema=schema or _settings.schema,
            odbcdriver_version=int(_settings.odbcdriver_version),
        )

        self._pool_config = merge_config_with_settings(MSSQLPoolConfig, pool_config, _settings)
        self._session_config = merge_config_with_settings(MSSQLSessionConfig, session_config, _settings)

        _ssl = ssl_config or MSSQLSSLConfig()
        self._ssl_config = MSSQLSSLConfig(
            ssl_encrypt=_ssl.ssl_encrypt if _ssl.ssl_encrypt is not None else _settings.ssl_encrypt,
            ssl_trust_server_certificate=(
                _ssl.ssl_trust_server_certificate
                if _ssl.ssl_trust_server_certificate is not None
                else _settings.ssl_trust_server_certificate
            ),
            ssl_ca_cert_path=(
                _ssl.ssl_ca_cert_path if _ssl.ssl_ca_cert_path is not None else _settings.ssl_ca_cert_path
            ),
        )

        self.sync_driver = _settings.sync_driver
        self.async_driver = _settings.async_driver

        _query = {
            "driver": f"ODBC Driver {self._connection_config.odbcdriver_version} for SQL Server",
            "Encrypt": "yes" if self._ssl_config.ssl_encrypt else "no",
            "TrustServerCertificate": "yes" if self._ssl_config.ssl_trust_server_certificate else "no",
        }
        if self._ssl_config.ssl_ca_cert_path:
            _query["ServerCertificate"] = self._ssl_config.ssl_ca_cert_path

        self.connection_url = {
            "host": self._connection_config.host,
            "port": self._connection_config.port,
            "database": self._connection_config.database,
            "username": self._connection_config.user,
            "password": self._connection_config.password,
            "query": _query,
        }

        self.extra_engine_args = extra_engine_args or {}
        self.engine_args = {
            "pool_size": self._pool_config.pool_size,
            "max_overflow": self._pool_config.max_overflow,
            "echo": self._session_config.echo,
            "pool_pre_ping": True,
            "pool_recycle": self._pool_config.pool_recycle,
            "connect_args": {
                "timeout": self._pool_config.connection_timeout,
                "login_timeout": self._pool_config.connection_timeout,
                "autocommit": self._session_config.autocommit,
            },
            **self.extra_engine_args,
        }

        self._connection_retry_config = merge_config_with_settings(
            MSSQLConnectionRetryConfig, connection_retry_config, _settings, CONNECTION_RETRY_FIELD_MAP
        )
        self._operation_retry_config = merge_config_with_settings(
            MSSQLOperationRetryConfig, operation_retry_config, _settings, OPERATION_RETRY_FIELD_MAP
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
            f"Initializing MSSQL(host={self._connection_config.host}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database})"
        )

    def __repr__(self) -> str:
        return (
            "MSSQL("
            f"host={self._connection_config.host!r}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database!r}, "
            f"pool_size={self._pool_config.pool_size}, "
            f"echo={self._session_config.echo}"
            ")"
        )

    def get_connection_info(self) -> MSSQLConnectionConfig:
        return self._connection_config

    def get_pool_info(self) -> MSSQLPoolConfig:
        return self._pool_config

    def get_session_info(self) -> MSSQLSessionConfig:
        return self._session_config

    def get_connection_retry_info(self) -> MSSQLConnectionRetryConfig:
        return self._connection_retry_config

    def get_operation_retry_info(self) -> MSSQLOperationRetryConfig:
        return self._operation_retry_config

    def get_ssl_info(self) -> MSSQLSSLConfig:
        return self._ssl_config
