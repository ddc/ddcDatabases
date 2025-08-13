import logging
import sys
from typing import Optional, Type
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.cursor import Cursor
from pymongo.errors import PyMongoError
from .settings import get_mongodb_settings


logger = logging.getLogger(__name__)
# Add NullHandler to prevent "No handlers found" warnings in libraries
logger.addHandler(logging.NullHandler())


class MongoDB:
    """
    Class to handle MongoDB connections
    """

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

        self.host = host or _settings.host
        self.port = port or _settings.port
        self.user = user or _settings.user
        self.password = password or _settings.password
        self.database = database or _settings.database
        self.collection = collection
        self.query = query or {}
        self.sort_column = sort_column
        self.sort_order = sort_order
        self.batch_size = batch_size or _settings.batch_size
        self.limit = limit or _settings.limit
        self.sync_driver = _settings.sync_driver
        self.is_connected = False
        self.client = None
        self.cursor_ref = None

        if not self.collection:
            raise ValueError("MongoDB collection name is required")

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
