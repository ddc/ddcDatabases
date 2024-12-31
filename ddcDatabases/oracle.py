# -*- encoding: utf-8 -*-
from typing import Optional
from .db_utils import BaseConnection
from .settings import OracleSettings


class Oracle(BaseConnection):
    """
    Class to handle Oracle connections
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        servicename: Optional[str] = None,
        echo: Optional[bool] = None,
        autoflush: Optional[bool] = None,
        expire_on_commit: Optional[bool] = None,
        extra_engine_args: Optional[dict] = None,
    ):
        _settings = OracleSettings()
        if not _settings.user or not _settings.password:
            raise RuntimeError("Missing username/password")

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
