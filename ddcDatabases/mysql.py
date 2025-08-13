from .db_utils import BaseConnection
from .settings import get_mysql_settings



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
        autocommit: bool | None = None,
        connection_timeout: int | None = None,
        pool_recycle: int | None = None,
        pool_size: int | None = None,
        max_overflow: int | None = None,
        extra_engine_args: dict | None = None,
    ):
        _settings = get_mysql_settings()

        self.echo = echo if echo is not None else _settings.echo
        self.autoflush = autoflush if autoflush is not None else _settings.autoflush
        self.expire_on_commit = expire_on_commit if expire_on_commit is not None else _settings.expire_on_commit
        self.autocommit = autocommit if autocommit is not None else _settings.autocommit
        self.connection_timeout = connection_timeout or _settings.connection_timeout
        self.pool_recycle = pool_recycle or _settings.pool_recycle
        self.pool_size = pool_size or _settings.pool_size
        self.max_overflow = max_overflow or _settings.max_overflow
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.connection_url = {
            "host": host or _settings.host,
            "port": int(port or _settings.port),
            "username": user or _settings.user,
            "password": password or _settings.password,
            "database": database or _settings.database,
        }
        self.extra_engine_args = extra_engine_args or {}
        self.engine_args = {
            "echo": self.echo,
            "pool_pre_ping": True,
            "pool_recycle": self.pool_recycle,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "connect_args": {
                "charset": "utf8mb4",
                "autocommit": self.autocommit,
                "connect_timeout": self.connection_timeout,
            },
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
