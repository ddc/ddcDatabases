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
        file_path: Optional[str] = None,
        echo: Optional[bool] = None,
    ):
        _settings = SQLiteSettings()
        self.temp_engine = None
        self.session = None
        self.file_path = file_path or _settings.file_path
        self.echo = echo or _settings.echo

    def __enter__(self):
        with self.engine() as self.temp_engine:
            session_maker = sessionmaker(bind=self.temp_engine,
                                         class_=Session,
                                         autoflush=True,
                                         expire_on_commit=True)

        with session_maker.begin() as self.session:
            return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            self.session.close()
        if self.temp_engine:
            self.temp_engine.dispose()

    @contextmanager
    def engine(self) -> Engine | None:
        try:
            _engine_args = {
                "url": f"sqlite:///{self.file_path}",
                "echo": self.echo,
            }
            _engine = create_engine(**_engine_args)
            yield _engine
        except Exception as e:
            dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            sys.stderr.write(
                f"[{dt}]:"
                "[ERROR]:Unable to Create Database Engine | "
                f"{repr(e)}"
            )
            raise
