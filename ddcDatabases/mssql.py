from .core.base import BaseConnection, ConnectionTester
from .core.configs import BaseConnectionConfig, PoolConfig, RetryConfig, SessionConfig
from .core.retry import RetryPolicy
from .core.settings import get_mssql_settings
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
import logging
from sqlalchemy.engine import URL, Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session
from typing import AsyncGenerator, Generator

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True)
class MSSQLConnectionConfig(BaseConnectionConfig):
    database: str | None = None
    schema: str | None = None
    odbcdriver_version: int | None = None


@dataclass(frozen=True)
class MSSQLSSLConfig:
    ssl_encrypt: bool | None = None
    ssl_trust_server_certificate: bool | None = None
    ssl_ca_cert_path: str | None = None


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
        pool_config: PoolConfig | None = None,
        session_config: SessionConfig | None = None,
        retry_config: RetryConfig | None = None,
        ssl_config: MSSQLSSLConfig | None = None,
        extra_engine_args: dict | None = None,
        logger: logging.Logger | None = None,
    ):
        _settings = get_mssql_settings()

        self._connection_config = MSSQLConnectionConfig(
            host=host or _settings.host,
            port=int(port or _settings.port),
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
            schema=schema or _settings.db_schema,
            odbcdriver_version=int(_settings.odbcdriver_version),
        )

        _pc = pool_config or PoolConfig()
        self._pool_config = PoolConfig(
            pool_size=_pc.pool_size if _pc.pool_size is not None else int(_settings.pool_size),
            max_overflow=_pc.max_overflow if _pc.max_overflow is not None else int(_settings.max_overflow),
            pool_recycle=_pc.pool_recycle if _pc.pool_recycle is not None else _settings.pool_recycle,
            connection_timeout=(
                _pc.connection_timeout if _pc.connection_timeout is not None else _settings.connection_timeout
            ),
        )

        _sc = session_config or SessionConfig()
        self._session_config = SessionConfig(
            echo=_sc.echo if _sc.echo is not None else _settings.echo,
            autoflush=_sc.autoflush if _sc.autoflush is not None else _settings.autoflush,
            expire_on_commit=_sc.expire_on_commit if _sc.expire_on_commit is not None else _settings.expire_on_commit,
            autocommit=_sc.autocommit if _sc.autocommit is not None else _settings.autocommit,
        )

        _ssl = ssl_config or MSSQLSSLConfig()
        self._ssl_config = MSSQLSSLConfig(
            ssl_encrypt=_ssl.ssl_encrypt if _ssl.ssl_encrypt is not None else _settings.ssl_encrypt,
            ssl_trust_server_certificate=(
                _ssl.ssl_trust_server_certificate
                if _ssl.ssl_trust_server_certificate is not None
                else _settings.ssl_trust_server_certificate
            ),
            ssl_ca_cert_path=_ssl.ssl_ca_cert_path,
        )

        self.sync_driver = _settings.sync_driver
        self.async_driver = _settings.async_driver

        self.connection_url = {
            "host": self._connection_config.host,
            "port": self._connection_config.port,
            "database": self._connection_config.database,
            "username": self._connection_config.user,
            "password": self._connection_config.password,
            "query": {
                "driver": f"ODBC Driver {self._connection_config.odbcdriver_version} for SQL Server",
                "Encrypt": "yes" if self._ssl_config.ssl_encrypt else "no",
                "TrustServerCertificate": "yes" if self._ssl_config.ssl_trust_server_certificate else "no",
            },
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

        # Create retry configuration
        _rc = retry_config or RetryConfig()
        self._retry_config = RetryConfig(
            enable_retry=_rc.enable_retry if _rc.enable_retry is not None else _settings.enable_retry,
            max_retries=_rc.max_retries if _rc.max_retries is not None else _settings.max_retries,
            initial_retry_delay=(
                _rc.initial_retry_delay if _rc.initial_retry_delay is not None else _settings.initial_retry_delay
            ),
            max_retry_delay=_rc.max_retry_delay if _rc.max_retry_delay is not None else _settings.max_retry_delay,
        )

        _retry_policy = RetryPolicy(
            enable_retry=self._retry_config.enable_retry,
            max_retries=self._retry_config.max_retries,
            initial_delay=self._retry_config.initial_retry_delay,
            max_delay=self._retry_config.max_retry_delay,
        )

        self.logger = logger if logger is not None else _logger

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self._session_config.autoflush,
            expire_on_commit=self._session_config.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
            retry_config=_retry_policy,
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
            f"schema={self._connection_config.schema!r}, "
            f"pool_size={self._pool_config.pool_size}, "
            f"echo={self._session_config.echo}"
            ")"
        )

    def get_connection_info(self) -> MSSQLConnectionConfig:
        return self._connection_config

    def get_pool_info(self) -> PoolConfig:
        return self._pool_config

    def get_session_info(self) -> SessionConfig:
        return self._session_config

    def get_retry_info(self) -> RetryConfig:
        return self._retry_config

    def get_ssl_info(self) -> MSSQLSSLConfig:
        return self._ssl_config

    @contextmanager
    def _get_engine(self) -> Generator[Engine, None, None]:
        _connection_url = URL.create(
            drivername=self.sync_driver,
            **self.connection_url,
        )
        _engine_args = {
            "url": _connection_url,
            **self.engine_args,
        }
        _engine = create_engine(**_engine_args)
        _engine.update_execution_options(schema_translate_map={None: self._connection_config.schema})
        yield _engine
        _engine.dispose()

    @asynccontextmanager
    async def _get_async_engine(self) -> AsyncGenerator[AsyncEngine, None]:
        _connection_url = URL.create(
            drivername=self.async_driver,
            **self.connection_url,
        )
        _engine_args = {
            "url": _connection_url,
            **self.engine_args,
        }
        _engine = create_async_engine(**_engine_args)
        _engine.update_execution_options(schema_translate_map={None: self._connection_config.schema})
        yield _engine
        await _engine.dispose()

    def _test_connection_sync(self, session: Session) -> None:
        del self.connection_url["password"]
        del self.connection_url["query"]
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.sync_driver,
            query={"schema": self._connection_config.schema},
        )
        test_connection = ConnectionTester(
            sync_session=session,
            host_url=_connection_url,
            logger=self.logger,
        )
        test_connection.test_connection_sync()

    async def _test_connection_async(self, session: AsyncSession) -> None:
        del self.connection_url["password"]
        del self.connection_url["query"]
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.async_driver,
            query={"schema": self._connection_config.schema},
        )
        test_connection = ConnectionTester(
            async_session=session,
            host_url=_connection_url,
            logger=self.logger,
        )
        await test_connection.test_connection_async()
