from .core.configs import BaseRetryConfig, BaseSessionConfig
from .core.retry import RetryPolicy, retry_operation
from .core.settings import get_sqlite_settings
from dataclasses import dataclass
import logging
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True, slots=True)
class SqliteSessionConfig(BaseSessionConfig):
    pass


@dataclass(frozen=True, slots=True)
class SqliteRetryConfig(BaseRetryConfig):
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
        retry_config: SqliteRetryConfig | None = None,
        extra_engine_args: dict[str, Any] | None = None,
        logger: logging.Logger | None = None,
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

        # Create retry configuration
        _rc = retry_config or SqliteRetryConfig()
        self._retry_config = SqliteRetryConfig(
            enable_retry=_rc.enable_retry if _rc.enable_retry is not None else _settings.enable_retry,
            max_retries=_rc.max_retries if _rc.max_retries is not None else _settings.max_retries,
            initial_retry_delay=(
                _rc.initial_retry_delay if _rc.initial_retry_delay is not None else _settings.initial_retry_delay
            ),
            max_retry_delay=_rc.max_retry_delay if _rc.max_retry_delay is not None else _settings.max_retry_delay,
        )

        self.retry_config = RetryPolicy(
            enable_retry=self._retry_config.enable_retry,
            max_retries=self._retry_config.max_retries,
            initial_delay=self._retry_config.initial_retry_delay,
            max_delay=self._retry_config.max_retry_delay,
        )

        self.logger = logger if logger is not None else _logger
        self.logger.debug(f"Initializing Sqlite(filepath={self.filepath})")

    def get_session_info(self) -> SqliteSessionConfig:
        """Get immutable session configuration."""
        return self._session_config

    def get_retry_info(self) -> SqliteRetryConfig:
        """Get immutable retry configuration."""
        return self._retry_config

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

        return retry_operation(connect, self.retry_config, "sqlite_connect", logger=self.logger)

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
