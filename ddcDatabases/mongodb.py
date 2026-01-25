from .core.configs import BaseConnectionConfig, BaseRetryConfig
from .core.retry import RetryPolicy, retry_operation
from .core.settings import get_mongodb_settings
from dataclasses import dataclass
import logging
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.cursor import Cursor
from pymongo.errors import PyMongoError
import sys
from typing import Any


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
class MongoDBRetryConfig(BaseRetryConfig):
    pass


_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


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
        retry_config: MongoDBRetryConfig | None = None,
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

        self.sync_driver = _settings.sync_driver
        self.is_connected = False
        self.client = None
        self.cursor_ref = None

        # Create retry configuration
        _rc = retry_config or MongoDBRetryConfig()
        self._retry_config = MongoDBRetryConfig(
            enable_retry=_rc.enable_retry if _rc.enable_retry is not None else _settings.enable_retry,
            max_retries=_rc.max_retries if _rc.max_retries is not None else _settings.max_retries,
            initial_retry_delay=(
                _rc.initial_retry_delay if _rc.initial_retry_delay is not None else _settings.initial_retry_delay
            ),
            max_retry_delay=_rc.max_retry_delay if _rc.max_retry_delay is not None else _settings.max_retry_delay,
        )

        self.retry_config = RetryPolicy(
            enable_retry=self._retry_config.enable_retry,
            max_retries=self._retry_config.max_retries,
            initial_delay=self._retry_config.initial_retry_delay,
            max_delay=self._retry_config.max_retry_delay,
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

    def get_retry_info(self) -> MongoDBRetryConfig:
        return self._retry_config

    def get_tls_info(self) -> MongoDBTLSConfig:
        return self._tls_config

    def __enter__(self) -> Cursor:
        def connect() -> Cursor:
            try:
                _connection_url = (
                    f"{self.sync_driver}://{self._connection_config.user}:{self._connection_config.password}"
                    f"@{self._connection_config.host}/{self._connection_config.database}"
                )
                if self._tls_config.tls_enabled:
                    _connection_url += "?tls=true"
                    if self._tls_config.tls_ca_cert_path:
                        _connection_url += f"&tlsCAFile={self._tls_config.tls_ca_cert_path}"
                    if self._tls_config.tls_cert_key_path:
                        _connection_url += f"&tlsCertificateKeyFile={self._tls_config.tls_cert_key_path}"
                    if self._tls_config.tls_allow_invalid_certificates:
                        _connection_url += "&tlsAllowInvalidCertificates=true"
                self.client = MongoClient(_connection_url)
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
            return retry_operation(connect, self.retry_config, "mongodb_connect", logger=self.logger)
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
                f"Connected to "
                f"{self._connection_config.user}@{self._connection_config.host}/"
                f"{self._connection_config.database}/{self._connection_config.collection}"
            )
        except PyMongoError as e:
            self.logger.error(
                f"Connection to MongoDB failed | "
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
