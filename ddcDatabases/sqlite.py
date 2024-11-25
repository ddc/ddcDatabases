# -*- encoding: utf-8 -*-
import sys
from datetime import datetime
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.orm import Session, sessionmaker
from .db_utils import DBUtils
from .settings import SQLiteSettings


class Sqlite:
    """
    Class to handle Sqlite connections
    """

    def __init__(
        self,
        file_path: str = None,
        echo: bool = None,
    ):
        _settings = SQLiteSettings()
        self.session = None
        self.file_path = _settings.file_path if not file_path else file_path
        self.echo = _settings.echo if not echo else echo

    def __enter__(self):
        engine = self._get_engine()
        session_maker = sessionmaker(bind=engine,
                                     class_=Session,
                                     autoflush=True,
                                     expire_on_commit=True)
        engine.dispose()
        with session_maker.begin() as session:
            self.session = session
            db_utils = DBUtils(self.session)
            return db_utils

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def _get_engine(self) -> Engine | None:
        try:
            _engine_args = {
                "url": f"sqlite:///{self.file_path}",
                "echo": self.echo,
            }

            engine = create_engine(**_engine_args)
            return engine
        except Exception as e:
            dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            sys.stderr.write(
                f"[{dt}]:[ERROR]:Unable to Create Database Engine | {repr(e)}"
            )
            raise
