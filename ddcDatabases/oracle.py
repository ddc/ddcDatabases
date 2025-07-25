# -*- encoding: utf-8 -*-
from ddcDatabases.db_utils import BaseConnection
from ddcDatabases.settings import get_oracle_settings


class Oracle(BaseConnection):
    """
    Class to handle Oracle connections
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        servicename: str | None = None,
        echo: bool | None = None,
        autoflush: bool | None = None,
        expire_on_commit: bool | None = None,
        extra_engine_args: dict | None = None,
    ):
        _settings = get_oracle_settings()

        self.echo = echo or _settings.echo
        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.sync_driver = _settings.sync_driver
        self.connection_url = {
            "host": host or _settings.host,
            "port": int(port or _settings.port),
            "username": user or _settings.user,
            "password": password or _settings.password,
            "query": {
                "service_name": servicename or _settings.servicename,
                "encoding": "UTF-8",
                "nencoding": "UTF-8",
            },
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
            async_driver=None,
        )
