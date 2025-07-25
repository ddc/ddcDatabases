from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from ddcDatabases.db_utils import BaseConnection, ConnectionTester
from ddcDatabases.settings import get_mssql_settings


class MSSQL(BaseConnection):
    """
    Class to handle MSSQL connections
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        echo: bool | None = None,
        pool_size: int | None = None,
        max_overflow: int | None = None,
        autoflush: bool | None = None,
        expire_on_commit: bool | None = None,
        extra_engine_args: dict | None = None,
    ):
        _settings = get_mssql_settings()

        self.schema = schema or _settings.db_schema
        self.echo = echo or _settings.echo
        self.pool_size = pool_size or int(_settings.pool_size)
        self.max_overflow = max_overflow or int(_settings.max_overflow)
        self.autoflush = autoflush
        self.expire_on_commit = expire_on_commit
        self.async_driver = _settings.async_driver
        self.sync_driver = _settings.sync_driver
        self.odbcdriver_version = int(_settings.odbcdriver_version)
        self.connection_url = {
            "host": host or _settings.host,
            "port": int(port or _settings.port),
            "database": database or _settings.database,
            "username": user or _settings.user,
            "password": password or _settings.password,
            "query": {
                "driver": f"ODBC Driver {self.odbcdriver_version} for SQL Server",
                "TrustServerCertificate": "yes",
            },
        }

        if not self.connection_url["username"] or not self.connection_url["password"]:
            raise RuntimeError("Missing username/password")
        self.extra_engine_args = extra_engine_args or {}
        self.engine_args = {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
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

    def _test_connection_sync(self, session: Session) -> None:
        del self.connection_url["password"]
        del self.connection_url["query"]
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.sync_driver,
            query={"schema": self.schema},
        )
        test_connection = ConnectionTester(
            sync_session=session,
            host_url=_connection_url,
        )
        test_connection.test_connection_sync()

    async def _test_connection_async(self, session: AsyncSession) -> None:
        del self.connection_url["password"]
        del self.connection_url["query"]
        _connection_url = URL.create(
            **self.connection_url,
            drivername=self.async_driver,
            query={"schema": self.schema},
        )
        test_connection = ConnectionTester(
            async_session=session,
            host_url=_connection_url,
        )
        await test_connection.test_connection_async()
