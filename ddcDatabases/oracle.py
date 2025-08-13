from .db_utils import BaseConnection
from .settings import get_oracle_settings


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
        autocommit: bool | None = None,
        connection_timeout: int | None = None,
        pool_recycle: int | None = None,
        pool_size: int | None = None,
        max_overflow: int | None = None,
        extra_engine_args: dict | None = None,
    ):
        _settings = get_oracle_settings()

        self.echo = echo if echo is not None else _settings.echo
        self.autoflush = autoflush if autoflush is not None else _settings.autoflush
        self.expire_on_commit = expire_on_commit if expire_on_commit is not None else _settings.expire_on_commit
        self.autocommit = autocommit if autocommit is not None else _settings.autocommit
        self.connection_timeout = connection_timeout or _settings.connection_timeout
        self.pool_recycle = pool_recycle or _settings.pool_recycle
        self.pool_size = pool_size or _settings.pool_size
        self.max_overflow = max_overflow or _settings.max_overflow
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
            "pool_pre_ping": True,
            "pool_recycle": self.pool_recycle,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "connect_args": {
                "threaded": True,
                "events": True,
                "autocommit": self.autocommit,
            },
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
