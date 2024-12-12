# -*- encoding: utf-8 -*-
from typing import Optional
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
        if not _settings.user or not _settings.password:
            raise RuntimeError("Missing username or password")

        self.echo = echo or _settings.echo
        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.connection_url = {
            "host": host or _settings.host,
            "port":  int(port or _settings.port),
            "database": database or _settings.database,
            "username": user or _settings.user,
            "password": password or _settings.password,
        }
        self.engine_args = {
            "echo": self.echo,
        }

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self.autoflush,
            expire_on_commit=self.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
        )
