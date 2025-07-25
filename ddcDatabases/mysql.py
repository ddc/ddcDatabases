# -*- encoding: utf-8 -*-
from ddcDatabases.db_utils import BaseConnection
from ddcDatabases.settings import get_mysql_settings


class MySQL(BaseConnection):
    """
    Class to handle MySQL connections
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        echo: bool | None = None,
        autoflush: bool | None = None,
        expire_on_commit: bool | None = None,
        extra_engine_args: dict | None = None,
    ):
        _settings = get_mysql_settings()

        self.echo = echo or _settings.echo
        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.connection_url = {
            "host": host or _settings.host,
            "port": int(port or _settings.port),
            "database": database or _settings.database,
            "username": user or _settings.user,
            "password": password or _settings.password,
        }

        if not self.connection_url["username"] or not self.connection_url["password"]:
            raise RuntimeError("Missing username/password")
        self.extra_engine_args = extra_engine_args or {}
        self.engine_args = {
            "echo": self.echo,
            **self.extra_engine_args,
        }

        super().__init__(
            connection_url=self.connection_url,
            engine_args=self.engine_args,
            autoflush=self.autoflush,
            expire_on_commit=self.expire_on_commit,
            sync_driver=self.sync_driver,
            async_driver=self.async_driver,
        )
