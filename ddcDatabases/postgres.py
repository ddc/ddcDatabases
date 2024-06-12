# -*- encoding: utf-8 -*-
import sys
import sqlalchemy as sa
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncEngine, AsyncSession, create_async_engine
from .exceptions import get_exception


class DBPostgres:
    """
    Class to handle postgres database connection

    database = DBPostgres(**db_configs)
    with database.session() as session:
        do your stuff here

    """

    def __init__(self, future=True, echo=False, drivername="psycopg2", **kwargs):
        self.echo = echo
        self.future = future
        self.drivername = drivername
        self.username = kwargs["username"]
        self.password = kwargs["password"]
        self.host = kwargs["host"]
        self.port = kwargs["port"]
        self.db = kwargs["database"]

    def uri(self) -> sa.engine.URL:
        credentials = {
            "drivername": f"postgresql+{self.drivername}",
            "username": self.username,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "database": self.db
        }
        return sa.engine.URL.create(**credentials)

    def engine(self) -> sa.Engine | None:
        try:
            engine = sa.create_engine(
                self.uri(),
                echo=self.echo,
                future=self.future
            )
            return engine
        except Exception as e:
            sys.stderr.write(f"Unable to Create Database Engine: {get_exception(e)}")
            return None

    def session(self, engine: sa.Engine = None) -> Session | None:
        _engine = engine or self.engine()
        if _engine is None:
            sys.stderr.write("Unable to Create Database Session: Empty Engine")
            return None
        session = sessionmaker(
            bind=_engine,
            autoflush=True,
            expire_on_commit=False,
            future=self.future,
            class_=Session
        )
        return session()


class DBPostgresAsync(DBPostgres):
    """
    Class to handle async postgres database connection

        database = DBPostgresAsync(**db_configs)
        async with database.session() as session:
            do your stuff here

    """

    def __init__(self, future=True, echo=False, drivername="asyncpg", ** kwargs):
        super().__init__(future, echo, drivername, **kwargs)

    def url(self) -> sa.engine.URL:
        return super().uri()

    def engine(self) -> AsyncEngine | None:
        try:
            engine = create_async_engine(
                self.url(),
                echo=self.echo,
                future=self.future
            )
            return engine
        except Exception as e:
            sys.stderr.write(f"Unable to Create Database Engine: {get_exception(e)}")
            return None

    def session(self, engine: AsyncEngine = None) -> AsyncSession | None:
        _engine = engine or self.engine()
        if _engine is None:
            sys.stderr.write("Unable to Create Database Session: Empty Engine")
            return None
        async_session = async_sessionmaker(
            bind=_engine,
            autoflush=True,
            expire_on_commit=False,
            future=self.future,
            class_=AsyncSession
        )
        return async_session()
