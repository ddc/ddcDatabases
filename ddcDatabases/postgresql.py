# -*- encoding: utf-8 -*-
from typing import Optional
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
)
from sqlalchemy.orm import Session
from .db_utils import BaseConn
from .settings import PostgreSQLSettings


class PostgreSQL(BaseConn):
    """
    Class to handle PostgreSQL connections
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        echo: Optional[bool] = None,
        autoflush: Optional[bool] = None,
        expire_on_commit: Optional[bool] = None,
    ):
        _settings = PostgreSQLSettings()
        self.host = host or _settings.host
        self.user = user or _settings.user
        self.password = password or _settings.password
        self.port = port or int(_settings.port)
        self.database = database or _settings.database
        self.echo = echo or _settings.echo

        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.connection_url = {
            "username": self.user,
            "password": self.password,
            "host": self.host,
            "port": self.port,
            "database": self.database,
        }
        self.engine_args = {
            "echo": self.echo,
        }

        if not self.user or not self.password:
            raise RuntimeError("Missing username or password")

        super().__init__(
            host=self.host,
            port=self.port,
            user=self.user,
            database=self.database,
            autoflush=self.autoflush,
            expire_on_commit=self.expire_on_commit,
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
        )
