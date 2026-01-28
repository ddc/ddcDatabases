import logging
import sys
from .core.configs import BaseConnectionConfig, BaseOperationRetryConfig, BaseRetryConfig
from .core.retry import retry_operation, retry_operation_async
from .core.settings import get_mongodb_settings
from dataclasses import dataclass
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCursor
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.cursor import Cursor
from pymongo.errors import PyMongoError
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


@dataclass(frozen=True, slots=True)
class MongoDBConnectionConfig(BaseConnectionConfig):
    database: str | None = None
    collection: str | None = None


@dataclass(frozen=True, slots=True)
class MongoDBTLSConfig:
    tls_enabled: bool | None = None
    tls_ca_cert_path: str | None = None
    tls_cert_key_path: str | None = None
    tls_allow_invalid_certificates: bool | None = None


@dataclass(frozen=True, slots=True)
class MongoDBQueryConfig:
    query: dict | None = None
    sort_column: str | None = None
    sort_order: str | None = None
    batch_size: int | None = None
    limit: int | None = None


@dataclass(frozen=True, slots=True)
class MongoDBConnRetryConfig(BaseRetryConfig):
    pass


@dataclass(frozen=True, slots=True)
class MongoDBOpRetryConfig(BaseOperationRetryConfig):
    pass


class MongoDB:
    """
    Class to handle MongoDB connections.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        collection: str | None = None,
        query_config: MongoDBQueryConfig | None = None,
        conn_retry_config: MongoDBConnRetryConfig | None = None,
        op_retry_config: MongoDBOpRetryConfig | None = None,
        tls_config: MongoDBTLSConfig | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        _settings = get_mongodb_settings()

        self._connection_config = MongoDBConnectionConfig(
            host=host or _settings.host,
            port=port or _settings.port,
            user=user or _settings.user,
            password=password or _settings.password,
            database=database or _settings.database,
            collection=collection,
        )

        _qc = query_config or MongoDBQueryConfig()
        self._query_config = MongoDBQueryConfig(
            query=_qc.query if _qc.query is not None else {},
            sort_column=_qc.sort_column,
            sort_order=_qc.sort_order,
            batch_size=_qc.batch_size if _qc.batch_size is not None else _settings.batch_size,
            limit=_qc.limit if _qc.limit is not None else _settings.limit,
        )

        _tls = tls_config or MongoDBTLSConfig()
        self._tls_config = MongoDBTLSConfig(
            tls_enabled=_tls.tls_enabled if _tls.tls_enabled is not None else _settings.tls_enabled,
            tls_ca_cert_path=_tls.tls_ca_cert_path if _tls.tls_ca_cert_path is not None else _settings.tls_ca_cert_path,
            tls_cert_key_path=(
                _tls.tls_cert_key_path if _tls.tls_cert_key_path is not None else _settings.tls_cert_key_path
            ),
            tls_allow_invalid_certificates=(
                _tls.tls_allow_invalid_certificates
                if _tls.tls_allow_invalid_certificates is not None
                else _settings.tls_allow_invalid_certificates
            ),
        )

        self.driver = _settings.driver
        self.is_connected = False
        self.client = None
        self.async_client = None
        self.cursor_ref = None
        self.async_cursor_ref = None

        # Create connection retry configuration
        _crc = conn_retry_config or MongoDBConnRetryConfig()
        self._conn_retry_config = MongoDBConnRetryConfig(
            enable_retry=_crc.enable_retry if _crc.enable_retry is not None else _settings.conn_enable_retry,
            max_retries=_crc.max_retries if _crc.max_retries is not None else _settings.conn_max_retries,
            initial_retry_delay=(
                _crc.initial_retry_delay if _crc.initial_retry_delay is not None else _settings.conn_initial_retry_delay
            ),
            max_retry_delay=(
                _crc.max_retry_delay if _crc.max_retry_delay is not None else _settings.conn_max_retry_delay
            ),
        )

        # Create operation retry configuration
        _orc = op_retry_config or MongoDBOpRetryConfig()
        self._op_retry_config = MongoDBOpRetryConfig(
            enable_retry=_orc.enable_retry if _orc.enable_retry is not None else _settings.op_enable_retry,
            max_retries=_orc.max_retries if _orc.max_retries is not None else _settings.op_max_retries,
            initial_retry_delay=(
                _orc.initial_retry_delay if _orc.initial_retry_delay is not None else _settings.op_initial_retry_delay
            ),
            max_retry_delay=_orc.max_retry_delay if _orc.max_retry_delay is not None else _settings.op_max_retry_delay,
            jitter=_orc.jitter if _orc.jitter is not None else _settings.op_jitter,
        )

        self.logger = logger if logger is not None else _logger

        if not self._connection_config.collection:
            raise ValueError("MongoDB collection name is required")

        self.logger.debug(
            f"Initializing MongoDB(host={self._connection_config.host}, "
            f"port={self._connection_config.port}, "
            f"database={self._connection_config.database}, "
            f"collection={self._connection_config.collection})"
        )

    def __repr__(self) -> str:
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

    def get_connection_info(self) -> MongoDBConnectionConfig:
        return self._connection_config

    def get_query_info(self) -> MongoDBQueryConfig:
        return self._query_config

    def get_conn_retry_info(self) -> MongoDBConnRetryConfig:
        return self._conn_retry_config

    def get_op_retry_info(self) -> MongoDBOpRetryConfig:
        return self._op_retry_config

    def get_tls_info(self) -> MongoDBTLSConfig:
        return self._tls_config

    def _build_connection_url(self) -> str:
        """Build the MongoDB connection URL with TLS options if configured."""
        url = (
            f"{self.driver}://{self._connection_config.user}:{self._connection_config.password}"
            f"@{self._connection_config.host}/{self._connection_config.database}"
        )
        if self._tls_config.tls_enabled:
            url += "?tls=true"
            if self._tls_config.tls_ca_cert_path:
                url += f"&tlsCAFile={self._tls_config.tls_ca_cert_path}"
            if self._tls_config.tls_cert_key_path:
                url += f"&tlsCertificateKeyFile={self._tls_config.tls_cert_key_path}"
            if self._tls_config.tls_allow_invalid_certificates:
                url += "&tlsAllowInvalidCertificates=true"
        return url

    def __enter__(self) -> Cursor:
        def connect() -> Cursor:
            try:
                self.client = MongoClient(self._build_connection_url())
                self._test_connection()
                self.is_connected = True
                self.cursor_ref = self._create_cursor(
                    self._connection_config.collection,
                    self._query_config.query,
                    self._query_config.sort_column,
                    self._query_config.sort_order,
                )
                return self.cursor_ref
            except (ConnectionError, RuntimeError, ValueError, TypeError):
                if self.client:
                    self.client.close()
                raise

        try:
            return retry_operation(connect, self._conn_retry_config, "mongodb_connect", logger=self.logger)
        except (PyMongoError, ConnectionError, RuntimeError, ValueError, TypeError):
            sys.exit(1)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self.cursor_ref:
            self.cursor_ref.close()
            self.cursor_ref = None
        if self.client:
            self.client.close()
            self.is_connected = False
        self.logger.debug("Disconnected")

    def _test_connection(self) -> None:
        try:
            self.client.admin.command("ping")
            self.logger.info(
                "Connected to "
                f"{self._connection_config.user}@{self._connection_config.host}/"
                f"{self._connection_config.database}/{self._connection_config.collection}"
            )
        except PyMongoError as e:
            self.logger.error(
                "Connection to MongoDB failed | "
                f"{self._connection_config.user}@{self._connection_config.host}/"
                f"{self._connection_config.database}/{self._connection_config.collection} | "
                f"{e}"
            )
            raise ConnectionError(f"Connection to MongoDB failed | {e}") from e

    def _create_cursor(
        self,
        collection: str,
        query: dict | None = None,
        sort_column: str | None = None,
        sort_order: str | None = None,
    ) -> Cursor:
        col = self.client[self._connection_config.database][collection]
        query = {} if query is None else query
        cursor = col.find(query, batch_size=self._query_config.batch_size, limit=self._query_config.limit)

        if sort_column is not None:
            sort_direction = DESCENDING if sort_order and sort_order.lower() in ["descending", "desc"] else ASCENDING
            if sort_column != "_id":
                col.create_index([(sort_column, sort_direction)])
            cursor = cursor.sort(sort_column, sort_direction)

        cursor.batch_size(self._query_config.batch_size)
        return cursor

    async def __aenter__(self) -> AsyncIOMotorCursor:
        async def connect() -> AsyncIOMotorCursor:
            try:
                self.async_client = AsyncIOMotorClient(self._build_connection_url())
                await self._test_connection_async()
                self.is_connected = True
                self.async_cursor_ref = self._create_cursor_async(
                    self._connection_config.collection,
                    self._query_config.query,
                    self._query_config.sort_column,
                    self._query_config.sort_order,
                )
                return self.async_cursor_ref
            except (ConnectionError, RuntimeError, ValueError, TypeError):
                if self.async_client:
                    self.async_client.close()
                raise

        try:
            return await retry_operation_async(
                connect, self._conn_retry_config, "mongodb_async_connect", logger=self.logger
            )
        except (PyMongoError, ConnectionError, RuntimeError, ValueError, TypeError):
            sys.exit(1)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        if self.async_cursor_ref:
            await self.async_cursor_ref.close()
            self.async_cursor_ref = None
        if self.async_client:
            self.async_client.close()
            self.is_connected = False
        self.logger.debug("Async disconnected")

    async def _test_connection_async(self) -> None:
        try:
            await self.async_client.admin.command("ping")
            self.logger.info(
                "Async connected to "
                f"{self._connection_config.user}@{self._connection_config.host}/"
                f"{self._connection_config.database}/{self._connection_config.collection}"
            )
        except PyMongoError as e:
            self.logger.error(
                "Async connection to MongoDB failed | "
                f"{self._connection_config.user}@{self._connection_config.host}/"
                f"{self._connection_config.database}/{self._connection_config.collection} | "
                f"{e}"
            )
            raise ConnectionError(f"Async connection to MongoDB failed | {e}") from e

    def _create_cursor_async(
        self,
        collection: str,
        query: dict | None = None,
        sort_column: str | None = None,
        sort_order: str | None = None,
    ) -> AsyncIOMotorCursor:
        col = self.async_client[self._connection_config.database][collection]
        query = {} if query is None else query
        cursor = col.find(query, batch_size=self._query_config.batch_size, limit=self._query_config.limit)

        if sort_column is not None:
            sort_direction = DESCENDING if sort_order and sort_order.lower() in ["descending", "desc"] else ASCENDING
            cursor = cursor.sort(sort_column, sort_direction)

        cursor.batch_size(self._query_config.batch_size)
        return cursor
