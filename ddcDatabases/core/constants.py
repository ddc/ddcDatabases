# SSL Modes
POSTGRESQL_SSL_MODES: frozenset[str] = frozenset(
    {
        "disable",
        "allow",
        "prefer",
        "require",
        "verify-ca",
        "verify-full",
    }
)

MYSQL_SSL_MODES: frozenset[str] = frozenset(
    {
        "DISABLED",
        "PREFERRED",
        "REQUIRED",
        "VERIFY_CA",
        "VERIFY_IDENTITY",
    }
)

# Connection error keywords for retry logic
CONNECTION_ERROR_KEYWORDS: frozenset[str] = frozenset(
    {
        "connection",
        "connect",
        "timeout",
        "timed out",
        "refused",
        "reset",
        "broken pipe",
        "network",
        "socket",
        "server closed",
        "lost connection",
        "server has gone away",
        "communication link",
        "operational error",
        "connection refused",
        "connection reset",
        "connection timed out",
        "no route to host",
        "host unreachable",
        "name or service not known",
        "temporary failure",
        "eof detected",
        "ssl error",
        "handshake failure",
        "authentication failed",
        "too many connections",
        "connection pool",
        "pool exhausted",
    }
)


class SettingsMessages:
    """Field description strings for settings"""

    ECHO_DESCRIPTION = "Enable SQLAlchemy query logging"
    AUTOFLUSH_DESCRIPTION = "Enable autoflush"
    EXPIRE_ON_COMMIT_DESCRIPTION = "Enable expire on commit"
    AUTOCOMMIT_DESCRIPTION = "Enable autocommit"
    CONNECTION_TIMEOUT_DESCRIPTION = "Connection timeout in seconds"
    POOL_RECYCLE_DESCRIPTION = "Pool recycle in seconds"
    POOL_SIZE_DESCRIPTION = "Database connection pool size"
    MAX_OVERFLOW_DESCRIPTION = "Maximum overflow connections for the pool"
    HOST_DESCRIPTION = "Database host"
    PORT_DESCRIPTION = "Database port"
    USERNAME_DESCRIPTION = "Database username"
    PASSWORD_DESCRIPTION = "Database password"
    NAME_DESCRIPTION = "Database name"
    SCHEMA_DESCRIPTION = "Database schema"
    ASYNC_DATABASE_DRIVER_DESCRIPTION = "Async database driver"
    SYNC_DATABASE_DRIVER_DESCRIPTION = "Sync database driver"

    # SSL settings descriptions
    SSL_ENABLED_DESCRIPTION = "Enable SSL/TLS connections"
    SSL_MODE_DESCRIPTION = "SSL mode for database connections"
    SSL_CA_CERT_PATH_DESCRIPTION = "Path to SSL CA certificate file"
    SSL_CLIENT_CERT_PATH_DESCRIPTION = "Path to SSL client certificate file"
    SSL_CLIENT_KEY_PATH_DESCRIPTION = "Path to SSL client key file"

    # Retry settings descriptions
    ENABLE_RETRY_DESCRIPTION = "Enable automatic retry on connection errors"
    MAX_RETRIES_DESCRIPTION = "Maximum number of retry attempts"
    INITIAL_RETRY_DELAY_DESCRIPTION = "Initial delay between retries in seconds"
    MAX_RETRY_DELAY_DESCRIPTION = "Maximum delay between retries in seconds"
    JITTER_DESCRIPTION = "Jitter factor for retry delays (0.0-1.0)"
    DISCONNECT_IDLE_TIMEOUT_DESCRIPTION = "Disconnect idle timeout in seconds for persistent connections"

    # SQLite specific
    SQLITE_FILE_PATH_DESCRIPTION = "Path to SQLite database file"

    # MSSQL specific
    ODBC_DRIVER_VERSION_DESCRIPTION = "ODBC driver version"
    SSL_ENCRYPT_DESCRIPTION = "Enable connection encryption"
    SSL_TRUST_SERVER_CERTIFICATE_DESCRIPTION = "Trust server certificate without validation"

    # MongoDB specific
    BATCH_SIZE_DESCRIPTION = "Batch size for operations"
    QUERY_LIMIT_DESCRIPTION = "Query result limit (0 = no limit)"
    MONGODB_DRIVER_DESCRIPTION = "MongoDB driver (used for both sync and async)"
    TLS_ENABLED_DESCRIPTION = "Enable TLS connections"
    TLS_CA_CERT_PATH_DESCRIPTION = "Path to TLS CA certificate file"
    TLS_CERT_KEY_PATH_DESCRIPTION = "Path to TLS client certificate/key file"
    TLS_ALLOW_INVALID_CERTIFICATES_DESCRIPTION = "Allow invalid TLS certificates"

    # Oracle specific
    SERVICE_NAME_DESCRIPTION = "Oracle service name"
    ORACLE_DRIVER_DESCRIPTION = "Oracle database driver"
    SSL_WALLET_PATH_DESCRIPTION = "Path to Oracle SSL wallet directory"
