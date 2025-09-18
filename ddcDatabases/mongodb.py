import logging
import sys
from dataclasses import dataclass
from typing import Optional, Type
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.cursor import Cursor
from pymongo.errors import PyMongoError
from .settings import get_mongodb_settings


@dataclass(slots=True, frozen=True)
class MongoConnectionConfig:
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None
    collection: str | None = None


@dataclass(slots=True, frozen=True)
class MongoQueryConfig:
    query: dict | None = None
    sort_column: str | None = None
    sort_order: str | None = None
    batch_size: int | None = None
    limit: int | None = None


logger = logging.getLogger(__name__)
# Add NullHandler to prevent "No handlers found" warnings in libraries
logger.addHandler(logging.NullHandler())


class MongoDB:
    """
    Class to handle MongoDB connections.
    """

    __slots__ = (
        'host',
        'port',
        'user',
        'password',
        'database',
        'collection',
        'query',
        'sort_column',
        'sort_order',
        'batch_size',
        'limit',
        'sync_driver',
        'is_connected',
        'client',
        'cursor_ref',
        '_connection_config',
        '_query_config',
    )

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        collection: str | None = None,
        query: dict | None = None,
        sort_column: str | None = None,
        sort_order: str | None = None,
        batch_size: int | None = None,
        limit: int | None = None,
    ):
        _settings = get_mongodb_settings()

        # Create configuration objects using dataclasses
        self._connection_config = MongoConnectionConfig(
            host=host or _settings.host,
            port=port or _settings.port,
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
            collection=collection,
        )

        self._query_config = MongoQueryConfig(
            query=query or {},
            sort_column=sort_column,
            sort_order=sort_order,
            batch_size=batch_size or _settings.batch_size,
            limit=limit or _settings.limit,
        )

        # Set instance attributes for backward compatibility
        self.host = self._connection_config.host
        self.port = self._connection_config.port
        self.user = self._connection_config.user
        self.password = self._connection_config.password
        self.database = self._connection_config.database
        self.collection = self._connection_config.collection
        self.query = self._query_config.query
        self.sort_column = self._query_config.sort_column
        self.sort_order = self._query_config.sort_order
        self.batch_size = self._query_config.batch_size
        self.limit = self._query_config.limit
        self.sync_driver = _settings.sync_driver
        self.is_connected = False
        self.client = None
        self.cursor_ref = None

        if not self.collection:
            raise ValueError("MongoDB collection name is required")

    def __repr__(self) -> str:
        """String representation using configuration objects."""
        return (
            "MongoDB("
            f"host={self._connection_config.host!r}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database!r}, "
            f"collection={self._connection_config.collection!r}, "
            f"batch_size={self._query_config.batch_size}, "
            f"limit={self._query_config.limit}"
            ")"
        )

    def get_connection_info(self) -> MongoConnectionConfig:
        """Get immutable connection configuration."""
        return self._connection_config

    def get_query_info(self) -> MongoQueryConfig:
        """Get immutable query configuration."""
        return self._query_config

    def __enter__(self) -> Cursor:
        try:
            _connection_url = f"{self.sync_driver}://{self.user}:{self.password}@{self.host}/{self.database}"
            self.client = MongoClient(_connection_url)
            self._test_connection()
            self.is_connected = True
            self.cursor_ref = self._create_cursor(self.collection, self.query, self.sort_column, self.sort_order)
            return self.cursor_ref
        except (ConnectionError, RuntimeError, ValueError, TypeError):
            self.client.close() if self.client else None
            sys.exit(1)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[object],
    ) -> None:
        if self.cursor_ref:
            self.cursor_ref.close()
            self.cursor_ref = None
        if self.client:
            self.client.close()
            self.is_connected = False

    def _test_connection(self) -> None:
        try:
            self.client.admin.command("ping")
            logger.info(
                f"Connection to database successful | {self.user}@{self.host}/{self.database}/{self.collection}"
            )
        except PyMongoError as e:
            logger.error(
                f"Connection to MongoDB failed | "
                f"{self.user}@{self.host}/{self.database}/{self.collection} | "
                f"{e}"
            )
            raise ConnectionError(f"Connection to MongoDB failed | {e}") from e

    def _create_cursor(
        self,
        collection: str,
        query: dict = None,
        sort_column: str = None,
        sort_order: str = None,
    ) -> Cursor:
        col = self.client[self.database][collection]
        query = {} if query is None else query
        cursor = col.find(query, batch_size=self.batch_size, limit=self.limit)

        if sort_column is not None:
            sort_direction = DESCENDING if sort_order and sort_order.lower() in ["descending", "desc"] else ASCENDING
            if sort_column != "_id":
                col.create_index([(sort_column, sort_direction)])
            cursor = cursor.sort(sort_column, sort_direction)

        cursor.batch_size(self.batch_size)
        return cursor
