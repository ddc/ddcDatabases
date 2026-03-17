import logging
from .core.configs import (
    CONNECTION_RETRY_FIELD_MAP,
    OPERATION_RETRY_FIELD_MAP,
    BaseOperationRetryConfig,
    BaseRetryConfig,
    BaseSessionConfig,
    merge_config_with_settings,
)
from .core.retry import retry_operation
from .core.settings import get_sqlite_settings
from dataclasses import dataclass
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True, slots=True)
class SqliteSessionConfig(BaseSessionConfig):
    pass


@dataclass(frozen=True, slots=True)
class SqliteConnectionRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class SqliteOperationRetryConfig(BaseOperationRetryConfig):
    pass


class Sqlite:
    """
    Class to handle Sqlite connections
    """

    def __init__(
        self,
        filepath: str | None = None,
        echo: bool | None = None,
        session_config: SqliteSessionConfig | None = None,
        connection_retry_config: SqliteConnectionRetryConfig | None = None,
        operation_retry_config: SqliteOperationRetryConfig | None = None,
        extra_engine_args: dict[str, Any] | None = None,
        logger: Any = None,
    ) -> None:
        _settings = get_sqlite_settings()

        self.filepath: str = filepath or _settings.file_path
        self.echo: bool = echo or _settings.echo

        _sc = session_config or SqliteSessionConfig()
        self._session_config = SqliteSessionConfig(
            autoflush=_sc.autoflush,
            expire_on_commit=_sc.expire_on_commit,
        )
        self.autoflush: bool | None = self._session_config.autoflush
        self.expire_on_commit: bool | None = self._session_config.expire_on_commit

        self.extra_engine_args: dict[str, Any] = extra_engine_args or {}
        self.is_connected: bool = False
        self.session: Session | None = None
        self._temp_engine: Engine | None = None

        self._connection_retry_config = merge_config_with_settings(
            SqliteConnectionRetryConfig, connection_retry_config, _settings, CONNECTION_RETRY_FIELD_MAP
        )
        self._operation_retry_config = merge_config_with_settings(
            SqliteOperationRetryConfig, operation_retry_config, _settings, OPERATION_RETRY_FIELD_MAP
        )

        self.logger = logger if logger is not None else _logger
        self.logger.debug(f"Initializing Sqlite(filepath={self.filepath})")

    def get_session_info(self) -> SqliteSessionConfig:
        """Get immutable session configuration."""
        return self._session_config

    def get_connection_retry_info(self) -> SqliteConnectionRetryConfig:
        """Get immutable connection retry configuration."""
        return self._connection_retry_config

    def get_operation_retry_info(self) -> SqliteOperationRetryConfig:
        """Get immutable operation retry configuration."""
        return self._operation_retry_config

    def __enter__(self) -> Session:
        def connect() -> Session:
            _engine_args = {
                "url": f"sqlite:///{self.filepath}",
                "echo": self.echo,
                **self.extra_engine_args,
            }
            try:
                engine = create_engine(**_engine_args)
            except Exception as e:
                self.logger.error(f"Unable to Create Database Engine | {e}")
                raise
            self._temp_engine = engine
            session_maker = sessionmaker(
                bind=engine,
                class_=Session,
                autoflush=self.autoflush or True,
                expire_on_commit=self.expire_on_commit or True,
            )
            with session_maker.begin() as self.session:
                self.is_connected = True
                self.logger.info(f"Connected to {self.filepath}")
                return self.session

        return retry_operation(connect, self._connection_retry_config, "sqlite_connect", logger=self.logger)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self.session:
            self.session.close()
        if self._temp_engine:
            self._temp_engine.dispose()
        self.is_connected = False
        self.logger.debug("Disconnected")
