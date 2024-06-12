# -*- encoding: utf-8 -*-
import sys
import sqlalchemy as sa
from sqlalchemy.engine import create_engine, Engine
from sqlalchemy.orm import Session, sessionmaker
from .exceptions import get_exception


class DBSqlite:
    """
    Class to handle sqlite databases

    database = DBSqlite(DATABASE_FILE_PATH)
    with database.session() as session:
        do your stuff here

    """

    def __init__(self, db_file_path: str, batch_size=100, echo=False, future=True):
        self.file = db_file_path
        self.batch_size = batch_size
        self.echo = echo
        self.future = future

    def url(self) -> str:
        return f"sqlite:///{self.file}"

    def engine(self) -> Engine | None:
        try:
            engine = create_engine(self.url(), future=self.future, echo=self.echo).\
                execution_options(stream_results=self.echo, isolation_level="AUTOCOMMIT")

            @sa.event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn,
                                              cursor,
                                              statement,
                                              params,
                                              context,
                                              executemany):
                cursor.arraysize = self.batch_size
            return engine
        except Exception as e:
            sys.stderr.write(f"Unable to Create Database Engine: {get_exception(e)}")
            return None

    def session(self, engine: Engine = None) -> Session | None:
        _engine = engine or self.engine()
        if _engine is None:
            sys.stderr.write("Unable to Create Database Session: Empty Engine")
            return None
        session_maker = sessionmaker(bind=_engine)
        _engine.dispose()
        return session_maker()
