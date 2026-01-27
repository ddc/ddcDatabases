from .core.configs import BaseOperationRetryConfig, BaseRetryConfig, BaseSessionConfig
from .core.retry import retry_operation
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
class SqliteConnRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class SqliteOpRetryConfig(BaseOperationRetryConfig):
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
        conn_retry_config: SqliteConnRetryConfig | None = None,
        op_retry_config: SqliteOpRetryConfig | None = None,
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

        # Create connection retry configuration
        _crc = conn_retry_config or SqliteConnRetryConfig()
        self._conn_retry_config = SqliteConnRetryConfig(
            enable_retry=_crc.enable_retry if _crc.enable_retry is not None else _settings.conn_enable_retry,
            max_retries=_crc.max_retries if _crc.max_retries is not None else _settings.conn_max_retries,
            initial_retry_delay=(
                _crc.initial_retry_delay if _crc.initial_retry_delay is not None else _settings.conn_initial_retry_delay
            ),
            max_retry_delay=(
                _crc.max_retry_delay if _crc.max_retry_delay is not None else _settings.conn_max_retry_delay
            ),
        )

        # Create operation retry configuration
        _orc = op_retry_config or SqliteOpRetryConfig()
        self._op_retry_config = SqliteOpRetryConfig(
            enable_retry=_orc.enable_retry if _orc.enable_retry is not None else _settings.op_enable_retry,
            max_retries=_orc.max_retries if _orc.max_retries is not None else _settings.op_max_retries,
            initial_retry_delay=(
                _orc.initial_retry_delay if _orc.initial_retry_delay is not None else _settings.op_initial_retry_delay
            ),
            max_retry_delay=_orc.max_retry_delay if _orc.max_retry_delay is not None else _settings.op_max_retry_delay,
            jitter=_orc.jitter if _orc.jitter is not None else _settings.op_jitter,
        )

        self.logger = logger if logger is not None else _logger
        self.logger.debug(f"Initializing Sqlite(filepath={self.filepath})")

    def get_session_info(self) -> SqliteSessionConfig:
        """Get immutable session configuration."""
        return self._session_config

    def get_conn_retry_info(self) -> SqliteConnRetryConfig:
        """Get immutable connection retry configuration."""
        return self._conn_retry_config

    def get_op_retry_info(self) -> SqliteOpRetryConfig:
        """Get immutable operation retry configuration."""
        return self._op_retry_config

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

        return retry_operation(connect, self._conn_retry_config, "sqlite_connect", logger=self.logger)

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
