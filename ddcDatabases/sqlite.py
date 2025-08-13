import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional, Type
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.orm import Session, sessionmaker
from .settings import get_sqlite_settings


logger = logging.getLogger(__name__)
# Add NullHandler to prevent "No handlers found" warnings in libraries
logger.addHandler(logging.NullHandler())


class Sqlite:
    """
    Class to handle Sqlite connections
    """

    def __init__(
        self,
        filepath: str | None = None,
        echo: bool | None = None,
        autoflush: bool | None = None,
        expire_on_commit: bool | None = None,
        extra_engine_args: dict[str, Any] | None = None,
    ) -> None:
        _settings = get_sqlite_settings()
        self.filepath: str = filepath or _settings.file_path
        self.echo: bool = echo or _settings.echo
        self.autoflush: bool | None = autoflush
        self.expire_on_commit: bool | None = expire_on_commit
        self.extra_engine_args: dict[str, Any] = extra_engine_args or {}
        self.is_connected: bool = False
        self.session: Session | None = None
        self._temp_engine: Engine | None = None

    def __enter__(self) -> Session:
        with self._get_engine() as self._temp_engine:
            session_maker = sessionmaker(
                bind=self._temp_engine,
                class_=Session,
                autoflush=self.autoflush or True,
                expire_on_commit=self.expire_on_commit or True,
            )

        with session_maker.begin() as self.session:
            self.is_connected = True
            return self.session

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[object],
    ) -> None:
        if self.session:
            self.session.close()
        if self._temp_engine:
            self._temp_engine.dispose()
        self.is_connected = False

    @contextmanager
    def _get_engine(self) -> Generator[Engine, None, None]:
        try:
            _engine_args = {
                "url": f"sqlite:///{self.filepath}",
                "echo": self.echo,
                **self.extra_engine_args,
            }
            _engine = create_engine(**_engine_args)
            yield _engine
            _engine.dispose()
        except Exception as e:
            logger.error(f"Unable to Create Database Engine | {e}")
            raise
