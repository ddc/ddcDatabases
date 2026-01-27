"""
Persistent database connection managers with automatic reconnection and idle timeout.

This module provides singleton-pattern connection managers that maintain
database connections across multiple operations, with automatic reconnection
on failure and idle timeout for resource management.
"""

from __future__ import annotations

from .configs import BaseRetryConfig
from .retry import retry_operation, retry_operation_async
from .settings import (
    get_mongodb_settings,
    get_mssql_settings,
    get_mysql_settings,
    get_oracle_settings,
    get_postgresql_settings,
)
from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass
import logging
from sqlalchemy import text
from sqlalchemy.engine import URL, Engine, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import Session, sessionmaker
import threading
import time
from typing import Any, Generic, TypeVar, cast
import weakref

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())

# Type variables
T = TypeVar('T')
SessionT = TypeVar('SessionT', Session, AsyncSession)


@dataclass(slots=True, frozen=True)
class PersistentConnectionConfig:
    """Configuration for persistent connections."""

    idle_timeout: int = 300  # seconds
    health_check_interval: int = 30  # seconds
    auto_reconnect: bool = True


# Global registry for persistent connections (weak references to allow cleanup)
_persistent_connections: weakref.WeakValueDictionary[str, "BasePersistentConnection | PersistentMongoDBConnection"] = (
    weakref.WeakValueDictionary()
)
_registry_lock = threading.Lock()


class IdleCheckerMixin:
    """
    Mixin providing idle connection checking functionality.

    Subclasses must define these attributes in __slots__:
        _connection_key, _config, _lock, _shutdown_event,
        _is_connected, _last_used, _idle_checker_thread
    """

    __slots__ = ()  # Mixin doesn't add slots; subclasses define them

    def _start_idle_checker(self: Any) -> None:
        """Start the background idle checker thread."""
        if self._idle_checker_thread is None or not self._idle_checker_thread.is_alive():
            self._shutdown_event.clear()
            self._idle_checker_thread = threading.Thread(
                target=self._idle_checker_loop,
                daemon=True,
                name=f"idle-checker-{self._connection_key}",
            )
            self._idle_checker_thread.start()

    def _idle_checker_loop(self: Any) -> None:
        """Background loop to check for idle connections and disconnect them."""
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(timeout=self._config.health_check_interval)

            if self._shutdown_event.is_set():
                break

            with self._lock:
                if self._is_connected:
                    idle_time = time.time() - self._last_used
                    if idle_time >= self._config.idle_timeout:
                        self._logger.info(
                            f"[{self._connection_key}] Connection idle for {idle_time:.0f}s, disconnecting..."
                        )
                        self._disconnect_internal()

    def _update_last_used(self: Any) -> None:
        """Update the last used timestamp."""
        self._last_used = time.time()

    def _disconnect_internal(self: Any) -> None:
        """Internal disconnect logic. Override in subclasses."""
        raise NotImplementedError


class BasePersistentConnection(IdleCheckerMixin, ABC, Generic[SessionT]):
    """
    Abstract base class for persistent database connections.

    Implements singleton pattern per connection key with:
    - Idle timeout with background checker thread
    - Automatic reconnection on failure
    - Thread-safe connection management
    """

    __slots__ = (
        '_connection_key',
        '_engine',
        '_session',
        '_last_used',
        '_lock',
        '_config',
        '_retry_config',
        '_idle_checker_thread',
        '_shutdown_event',
        '_is_connected',
        '_logger',
        '__weakref__',  # Required for WeakValueDictionary
    )

    def __init__(
        self,
        connection_key: str,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._connection_key = connection_key
        self._config = config or PersistentConnectionConfig()
        self._retry_config = retry_config or BaseRetryConfig()
        self._engine: Engine | AsyncEngine | None = None
        self._session: SessionT | None = None
        self._last_used = time.time()
        self._lock = threading.RLock()
        self._shutdown_event = threading.Event()
        self._is_connected = False
        self._idle_checker_thread: threading.Thread | None = None
        self._logger = logger if logger is not None else _logger

    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._is_connected

    @property
    def connection_key(self) -> str:
        """Get the connection key."""
        return self._connection_key

    @abstractmethod
    def _create_engine(self) -> Engine | AsyncEngine:
        """Create the database engine. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def _create_session(self, engine: Engine | AsyncEngine) -> SessionT:
        """Create a session from the engine. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def _disconnect_internal(self) -> None:
        """Internal disconnect logic. Must be implemented by subclasses."""
        pass

    @abstractmethod
    def connect(self) -> SessionT:
        """Connect to the database and return a session."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the database."""
        pass

    def shutdown(self) -> None:
        """Shutdown the persistent connection and stop background threads."""
        self._shutdown_event.set()
        self.disconnect()
        if self._idle_checker_thread and self._idle_checker_thread.is_alive():
            self._idle_checker_thread.join(timeout=5)


class PersistentSQLAlchemyConnection(BasePersistentConnection[Session]):
    """
    Persistent connection manager for synchronous SQLAlchemy databases.

    Supports PostgreSQL, MySQL, MSSQL, and Oracle.
    """

    __slots__ = (
        '_connection_url',
        '_engine_args',
        '_autoflush',
        '_expire_on_commit',
    )

    def __init__(
        self,
        connection_key: str,
        connection_url: URL | str,
        engine_args: dict[str, Any] | None = None,
        autoflush: bool = False,
        expire_on_commit: bool = False,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(connection_key, config, retry_config, logger)
        self._connection_url = connection_url
        self._engine_args = engine_args or {}
        self._autoflush = autoflush
        self._expire_on_commit = expire_on_commit

    def _create_engine(self) -> Engine:
        """Create the SQLAlchemy engine."""
        return create_engine(
            self._connection_url,
            **self._engine_args,
        )

    def _create_session(self, engine: Engine) -> Session:
        """Create a session from the engine."""
        session_factory = sessionmaker(
            bind=engine,
            autoflush=self._autoflush,
            expire_on_commit=self._expire_on_commit,
        )
        return session_factory()

    def _disconnect_internal(self) -> None:
        """Internal disconnect logic."""
        if self._session:
            try:
                self._session.close()
            except Exception as e:
                self._logger.warning(f"[{self._connection_key}] Error closing session: {e}")
            self._session = None

        if self._engine:
            try:
                self._engine.dispose()
            except Exception as e:
                self._logger.warning(f"[{self._connection_key}] Error disposing engine: {e}")
            self._engine = None

        self._is_connected = False

    def connect(self) -> Session:
        """
        Connect to the database and return a session.

        Uses retry logic for connection attempts.
        """
        with self._lock:
            self._update_last_used()

            if self._is_connected and self._session:
                # Verify connection is still valid
                try:
                    self._session.execute(text("SELECT 1"))
                    return self._session
                except SQLAlchemyError:
                    self._logger.warning(f"[{self._connection_key}] Connection lost, reconnecting...")
                    self._disconnect_internal()

            def do_connect() -> Session:
                self._engine = self._create_engine()
                self._session = self._create_session(self._engine)
                self._is_connected = True
                self._start_idle_checker()
                self._logger.info(f"[{self._connection_key}] Connected successfully")
                return self._session

            if self._config.auto_reconnect:
                return retry_operation(
                    do_connect,
                    self._retry_config,
                    f"{self._connection_key}_connect",
                    logger=self._logger,
                )
            else:
                return do_connect()

    def disconnect(self) -> None:
        """Disconnect from the database."""
        with self._lock:
            self._disconnect_internal()
            self._logger.info(f"[{self._connection_key}] Disconnected")

    def __enter__(self) -> Session:
        """Context manager entry."""
        return self.connect()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Context manager exit - updates last used but doesn't disconnect."""
        self._update_last_used()


class PersistentSQLAlchemyAsyncConnection(BasePersistentConnection[AsyncSession]):
    """
    Persistent connection manager for asynchronous SQLAlchemy databases.

    Supports PostgreSQL, MySQL, and MSSQL with async drivers.
    """

    __slots__ = (
        '_connection_url',
        '_engine_args',
        '_autoflush',
        '_expire_on_commit',
        '_async_lock',
    )

    def __init__(
        self,
        connection_key: str,
        connection_url: URL | str,
        engine_args: dict[str, Any] | None = None,
        autoflush: bool = False,
        expire_on_commit: bool = False,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(connection_key, config, retry_config, logger)
        self._connection_url = connection_url
        self._engine_args = engine_args or {}
        self._autoflush = autoflush
        self._expire_on_commit = expire_on_commit
        self._async_lock = asyncio.Lock()

    def _create_engine(self) -> AsyncEngine:
        """Create the async SQLAlchemy engine."""
        return create_async_engine(
            self._connection_url,
            **self._engine_args,
        )

    def _create_session(self, engine: AsyncEngine) -> AsyncSession:
        """Create an async session from the engine."""
        from sqlalchemy.ext.asyncio import async_sessionmaker

        session_factory = async_sessionmaker(
            bind=engine,
            autoflush=self._autoflush,
            expire_on_commit=self._expire_on_commit,
        )
        return session_factory()

    def _disconnect_internal(self) -> None:
        """Internal disconnect logic (sync version for idle checker)."""
        # Note: This is called from the sync idle checker thread
        # Async cleanup will happen lazily on next connect
        self._session = None
        self._engine = None
        self._is_connected = False

    async def _async_disconnect_internal(self) -> None:
        """Internal async disconnect logic."""
        if self._session:
            try:
                await self._session.close()
            except Exception as e:
                self._logger.warning(f"[{self._connection_key}] Error closing session: {e}")
            self._session = None

        if self._engine:
            try:
                await self._engine.dispose()
            except Exception as e:
                self._logger.warning(f"[{self._connection_key}] Error disposing engine: {e}")
            self._engine = None

        self._is_connected = False

    def connect(self) -> AsyncSession:
        """Sync connect raises error - use async_connect instead."""
        raise NotImplementedError("Use async_connect() for async connections")

    async def async_connect(self) -> AsyncSession:
        """
        Connect to the database asynchronously and return a session.

        Uses retry logic for connection attempts.
        """
        async with self._async_lock:
            self._update_last_used()

            if self._is_connected and self._session:
                # Verify connection is still valid
                try:
                    await self._session.execute(text("SELECT 1"))
                    return self._session
                except SQLAlchemyError:
                    self._logger.warning(f"[{self._connection_key}] Connection lost, reconnecting...")
                    await self._async_disconnect_internal()

            async def do_connect() -> AsyncSession:
                await asyncio.sleep(0)  # Yield to event loop
                self._engine = self._create_engine()
                self._session = self._create_session(self._engine)
                self._is_connected = True
                self._start_idle_checker()
                self._logger.info(f"[{self._connection_key}] Connected successfully")
                return self._session

            if self._config.auto_reconnect:
                return await retry_operation_async(
                    do_connect,
                    self._retry_config,
                    f"{self._connection_key}_async_connect",
                    logger=self._logger,
                )
            else:
                return await do_connect()

    def disconnect(self) -> None:
        """Sync disconnect - marks as disconnected."""
        with self._lock:
            self._disconnect_internal()
            self._logger.info(f"[{self._connection_key}] Disconnected (sync)")

    async def async_disconnect(self) -> None:
        """Disconnect from the database asynchronously."""
        async with self._async_lock:
            await self._async_disconnect_internal()
            self._logger.info(f"[{self._connection_key}] Disconnected")

    async def __aenter__(self) -> AsyncSession:
        """Async context manager entry."""
        return await self.async_connect()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit - updates last used but doesn't disconnect."""
        self._update_last_used()


class PersistentMongoDBConnection(IdleCheckerMixin):
    """
    Persistent connection manager for MongoDB.

    Maintains a MongoClient connection with automatic reconnection.
    """

    __slots__ = (
        '_connection_key',
        '_connection_url',
        '_database',
        '_client',
        '_db',
        '_last_used',
        '_lock',
        '_config',
        '_retry_config',
        '_idle_checker_thread',
        '_shutdown_event',
        '_is_connected',
        '_logger',
        '__weakref__',  # Required for WeakValueDictionary
    )

    def __init__(
        self,
        connection_key: str,
        connection_url: str,
        database: str,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._connection_key = connection_key
        self._connection_url = connection_url
        self._database = database
        self._config = config or PersistentConnectionConfig()
        self._retry_config = retry_config or BaseRetryConfig()
        self._client = None
        self._db = None
        self._last_used = time.time()
        self._lock = threading.RLock()
        self._shutdown_event = threading.Event()
        self._is_connected = False
        self._idle_checker_thread: threading.Thread | None = None
        self._logger = logger if logger is not None else _logger

    @property
    def is_connected(self) -> bool:
        """Check if currently connected."""
        return self._is_connected

    @property
    def connection_key(self) -> str:
        """Get the connection key."""
        return self._connection_key

    def _disconnect_internal(self) -> None:
        """Internal disconnect logic."""
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                self._logger.warning(f"[{self._connection_key}] Error closing client: {e}")
            self._client = None
            self._db = None
        self._is_connected = False

    def connect(self) -> Any:
        """
        Connect to MongoDB and return the database handle.

        Uses retry logic for connection attempts.
        """
        from pymongo import MongoClient
        from pymongo.errors import PyMongoError

        with self._lock:
            self._update_last_used()

            if self._is_connected and self._client and self._db:
                # Verify connection is still valid
                try:
                    self._client.admin.command("ping")
                    return self._db
                except PyMongoError:
                    self._logger.warning(f"[{self._connection_key}] Connection lost, reconnecting...")
                    self._disconnect_internal()

            def do_connect() -> Any:
                self._client = MongoClient(self._connection_url)
                self._client.admin.command("ping")  # Verify connection
                self._db = self._client[self._database]
                self._is_connected = True
                self._start_idle_checker()
                self._logger.info(f"[{self._connection_key}] Connected successfully")
                return self._db

            if self._config.auto_reconnect:
                return retry_operation(
                    do_connect,
                    self._retry_config,
                    f"{self._connection_key}_connect",
                    logger=self._logger,
                )
            else:
                return do_connect()

    def disconnect(self) -> None:
        """Disconnect from MongoDB."""
        with self._lock:
            self._disconnect_internal()
            self._logger.info(f"[{self._connection_key}] Disconnected")

    def shutdown(self) -> None:
        """Shutdown the persistent connection and stop background threads."""
        self._shutdown_event.set()
        self.disconnect()
        if self._idle_checker_thread and self._idle_checker_thread.is_alive():
            self._idle_checker_thread.join(timeout=5)

    def __enter__(self) -> Any:
        """Context manager entry."""
        return self.connect()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Context manager exit - updates last used but doesn't disconnect."""
        self._update_last_used()


# Database-specific persistent connection classes with singleton pattern


class PostgreSQLPersistent:
    """
    Persistent PostgreSQL connection with singleton pattern.

    Same connection parameters return the same connection instance.
    Supports both sync and async modes.

    Example:
        # Synchronous
        conn = PostgreSQLPersistent(host="localhost", database="mydb")
        with conn as session:
            # use session

        # Asynchronous
        conn = PostgreSQLPersistent(host="localhost", database="mydb", async_mode=True)
        async with conn as session:
            # use async session
    """

    def __new__(
        cls,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        async_mode: bool = False,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
        **engine_kwargs: Any,
    ) -> PersistentSQLAlchemyConnection | PersistentSQLAlchemyAsyncConnection:
        """Create or return existing persistent PostgreSQL connection."""
        _settings = get_postgresql_settings()
        host = host or _settings.host
        port = port or int(_settings.port)
        user = user or _settings.user
        password = password or _settings.password
        database = database or _settings.database
        connection_key = f"postgresql://{user}@{host}:{port}/{database}"  # NOSONAR

        with _registry_lock:
            if connection_key in _persistent_connections:
                return cast(
                    PersistentSQLAlchemyConnection | PersistentSQLAlchemyAsyncConnection,
                    _persistent_connections[connection_key],
                )

            if async_mode:
                connection_url = URL.create(
                    drivername="postgresql+asyncpg",
                    username=user,
                    password=password,
                    host=host,
                    port=port,
                    database=database,
                )
                conn = PersistentSQLAlchemyAsyncConnection(
                    connection_key=connection_key,
                    connection_url=connection_url,
                    engine_args=engine_kwargs,
                    config=config,
                    retry_config=retry_config,
                )
            else:
                connection_url = URL.create(
                    drivername="postgresql+psycopg2",
                    username=user,
                    password=password,
                    host=host,
                    port=port,
                    database=database,
                )
                conn = PersistentSQLAlchemyConnection(
                    connection_key=connection_key,
                    connection_url=connection_url,
                    engine_args=engine_kwargs,
                    config=config,
                    retry_config=retry_config,
                )

            _persistent_connections[connection_key] = conn
            return conn


class MySQLPersistent:
    """
    Persistent MySQL/MariaDB connection with singleton pattern.

    Same connection parameters return the same connection instance.
    Supports both sync and async modes.

    Example:
        # Synchronous
        conn = MySQLPersistent(host="localhost", database="mydb")
        with conn as session:
            # use session

        # Asynchronous
        conn = MySQLPersistent(host="localhost", database="mydb", async_mode=True)
        async with conn as session:
            # use async session
    """

    def __new__(
        cls,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        async_mode: bool = False,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
        **engine_kwargs: Any,
    ) -> PersistentSQLAlchemyConnection | PersistentSQLAlchemyAsyncConnection:
        """Create or return existing persistent MySQL connection."""
        _settings = get_mysql_settings()
        host = host or _settings.host
        port = port or int(_settings.port)
        user = user or _settings.user
        password = password or _settings.password
        database = database or _settings.database
        connection_key = f"mysql://{user}@{host}:{port}/{database}"  # NOSONAR

        with _registry_lock:
            if connection_key in _persistent_connections:
                return cast(
                    PersistentSQLAlchemyConnection | PersistentSQLAlchemyAsyncConnection,
                    _persistent_connections[connection_key],
                )

            if async_mode:
                connection_url = URL.create(
                    drivername="mysql+aiomysql",
                    username=user,
                    password=password,
                    host=host,
                    port=port,
                    database=database,
                )
                conn = PersistentSQLAlchemyAsyncConnection(
                    connection_key=connection_key,
                    connection_url=connection_url,
                    engine_args=engine_kwargs,
                    config=config,
                    retry_config=retry_config,
                )
            else:
                connection_url = URL.create(
                    drivername="mysql+pymysql",
                    username=user,
                    password=password,
                    host=host,
                    port=port,
                    database=database,
                )
                conn = PersistentSQLAlchemyConnection(
                    connection_key=connection_key,
                    connection_url=connection_url,
                    engine_args=engine_kwargs,
                    config=config,
                    retry_config=retry_config,
                )

            _persistent_connections[connection_key] = conn
            return conn


class MSSQLPersistent:
    """
    Persistent MSSQL (SQL Server) connection with singleton pattern.

    Same connection parameters return the same connection instance.
    Supports both sync and async modes.

    Example:
        # Synchronous
        conn = MSSQLPersistent(host="localhost", database="master")
        with conn as session:
            # use session

        # Asynchronous
        conn = MSSQLPersistent(host="localhost", database="master", async_mode=True)
        async with conn as session:
            # use async session
    """

    def __new__(
        cls,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        async_mode: bool = False,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
        **engine_kwargs: Any,
    ) -> PersistentSQLAlchemyConnection | PersistentSQLAlchemyAsyncConnection:
        """Create or return existing persistent MSSQL connection."""
        _settings = get_mssql_settings()
        host = host or _settings.host
        port = port or int(_settings.port)
        user = user or _settings.user
        password = password or _settings.password
        database = database or _settings.database
        connection_key = f"mssql://{user}@{host}:{port}/{database}"  # NOSONAR

        with _registry_lock:
            if connection_key in _persistent_connections:
                return cast(
                    PersistentSQLAlchemyConnection | PersistentSQLAlchemyAsyncConnection,
                    _persistent_connections[connection_key],
                )

            if async_mode:
                connection_url = URL.create(
                    drivername="mssql+aioodbc",
                    username=user,
                    password=password,
                    host=host,
                    port=port,
                    database=database,
                    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"},
                )
                conn = PersistentSQLAlchemyAsyncConnection(
                    connection_key=connection_key,
                    connection_url=connection_url,
                    engine_args=engine_kwargs,
                    config=config,
                    retry_config=retry_config,
                )
            else:
                connection_url = URL.create(
                    drivername="mssql+pyodbc",
                    username=user,
                    password=password,
                    host=host,
                    port=port,
                    database=database,
                    query={"driver": "ODBC Driver 18 for SQL Server", "TrustServerCertificate": "yes"},
                )
                conn = PersistentSQLAlchemyConnection(
                    connection_key=connection_key,
                    connection_url=connection_url,
                    engine_args=engine_kwargs,
                    config=config,
                    retry_config=retry_config,
                )

            _persistent_connections[connection_key] = conn
            return conn


class OraclePersistent:
    """
    Persistent Oracle connection with singleton pattern.

    Same connection parameters return the same connection instance.
    Note: Oracle only supports synchronous connections.

    Example:
        conn = OraclePersistent(host="localhost", servicename="xe")
        with conn as session:
            # use session
    """

    def __new__(
        cls,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        servicename: str | None = None,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
        **engine_kwargs: Any,
    ) -> PersistentSQLAlchemyConnection:
        """Create or return existing persistent Oracle connection."""
        _settings = get_oracle_settings()
        host = host or _settings.host
        port = port or int(_settings.port)
        user = user or _settings.user
        password = password or _settings.password
        servicename = servicename or _settings.servicename
        connection_key = f"oracle://{user}@{host}:{port}/{servicename}"  # NOSONAR

        with _registry_lock:
            if connection_key in _persistent_connections:
                return cast(PersistentSQLAlchemyConnection, _persistent_connections[connection_key])

            connection_url = URL.create(
                drivername="oracle+oracledb",
                username=user,
                password=password,
                host=host,
                port=port,
                query={"service_name": servicename},
            )
            conn = PersistentSQLAlchemyConnection(
                connection_key=connection_key,
                connection_url=connection_url,
                engine_args=engine_kwargs,
                config=config,
                retry_config=retry_config,
            )

            _persistent_connections[connection_key] = conn
            return conn


class MongoDBPersistent:
    """
    Persistent MongoDB connection with singleton pattern.

    Same connection parameters return the same connection instance.
    Note: MongoDB only supports synchronous connections.

    Example:
        conn = MongoDBPersistent(host="localhost", database="admin")
        with conn as db:
            # use db handle
    """

    def __new__(
        cls,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        config: PersistentConnectionConfig | None = None,
        retry_config: BaseRetryConfig | None = None,
    ) -> PersistentMongoDBConnection:
        """Create or return existing persistent MongoDB connection."""
        _settings = get_mongodb_settings()
        host = host or _settings.host
        port = port or int(_settings.port)
        user = user or _settings.user
        password = password or _settings.password
        database = database or _settings.database
        connection_key = f"mongodb://{user}@{host}:{port}/{database}"  # NOSONAR

        with _registry_lock:
            if connection_key in _persistent_connections:
                return cast(PersistentMongoDBConnection, cast(object, _persistent_connections[connection_key]))

            connection_url = f"mongodb://{user}:{password}@{host}:{port}/{database}"
            conn = PersistentMongoDBConnection(
                connection_key=connection_key,
                connection_url=connection_url,
                database=database,
                config=config,
                retry_config=retry_config,
            )

            _persistent_connections[connection_key] = conn
            return conn


def close_all_persistent_connections() -> None:
    """Close all persistent connections and clean up resources."""
    with _registry_lock:
        for key, conn in tuple(_persistent_connections.items()):
            try:
                conn.shutdown()
            except Exception as e:
                _logger.warning(f"Error shutting down connection {key}: {e}")
        _persistent_connections.clear()
