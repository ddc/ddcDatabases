# -*- encoding: utf-8 -*-
import sys
from contextlib import contextmanager
from datetime import datetime
from typing import Optional
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.orm import Session, sessionmaker
from .settings import SQLiteSettings


class Sqlite:
    """
    Class to handle Sqlite connections
    """

    def __init__(
        self,
        filepath: Optional[str] = None,
        echo: Optional[bool] = None,
        autoflush: Optional[bool] = None,
        expire_on_commit: Optional[bool] = None,
        extra_engine_args: Optional[dict] = None,
    ):
        _settings = SQLiteSettings()
        self.filepath = filepath or _settings.file_path
        self.echo = echo or _settings.echo
        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.extra_engine_args = extra_engine_args or {}
        self.is_connected = False
        self.session = None
        self._temp_engine = None


    def __enter__(self):
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

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
        if self._temp_engine:
            self._temp_engine.dispose()
        self.is_connected = False

    @contextmanager
    def _get_engine(self) -> Engine | None:
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
            dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            sys.stderr.write(
                f"[{dt}]:[ERROR]:Unable to Create Database Engine | "
                f"{repr(e)}\n"
            )
            raise
