import logging
from .core.operations import DBUtils, DBUtilsAsync
from .core.persistent import PersistentConnectionConfig, close_all_persistent_connections
from importlib.metadata import version

__all__ = [
    "DBUtils",
    "DBUtilsAsync",
    "PersistentConnectionConfig",
    "close_all_persistent_connections",
]

# Conditional imports based on available dependencies
try:
    from .core.settings import clear_sqlite_settings_cache, get_sqlite_settings
    from .sqlite import (
        Sqlite,
        SqliteConnRetryConfig,
        SqliteOpRetryConfig,
        SqliteSessionConfig,
    )

    __all__ += [
        "Sqlite",
        "SqliteConnRetryConfig",
        "SqliteOpRetryConfig",
        "SqliteSessionConfig",
        "clear_sqlite_settings_cache",
        "get_sqlite_settings",
    ]
except ImportError:
    pass

try:
    from .core.persistent import MongoDBPersistent
    from .core.settings import clear_mongodb_settings_cache, get_mongodb_settings
    from .mongodb import (
        MongoDB,
        MongoDBConnectionConfig,
        MongoDBConnRetryConfig,
        MongoDBOpRetryConfig,
        MongoDBQueryConfig,
        MongoDBTLSConfig,
    )

    __all__ += [
        "MongoDB",
        "MongoDBConnectionConfig",
        "MongoDBConnRetryConfig",
        "MongoDBOpRetryConfig",
        "MongoDBPersistent",
        "MongoDBQueryConfig",
        "MongoDBTLSConfig",
        "clear_mongodb_settings_cache",
        "get_mongodb_settings",
    ]
except ImportError:
    pass

try:
    from .core.persistent import MSSQLPersistent
    from .core.settings import clear_mssql_settings_cache, get_mssql_settings
    from .mssql import (
        MSSQL,
        MSSQLConnectionConfig,
        MSSQLConnRetryConfig,
        MSSQLOpRetryConfig,
        MSSQLPoolConfig,
        MSSQLSessionConfig,
        MSSQLSSLConfig,
    )

    __all__ += [
        "MSSQL",
        "MSSQLConnectionConfig",
        "MSSQLConnRetryConfig",
        "MSSQLOpRetryConfig",
        "MSSQLPersistent",
        "MSSQLPoolConfig",
        "MSSQLSessionConfig",
        "MSSQLSSLConfig",
        "clear_mssql_settings_cache",
        "get_mssql_settings",
    ]
except ImportError:
    pass

try:
    from .core.persistent import MySQLPersistent
    from .core.settings import clear_mysql_settings_cache, get_mysql_settings
    from .mysql import (
        MySQL,
        MySQLConnectionConfig,
        MySQLConnRetryConfig,
        MySQLOpRetryConfig,
        MySQLPoolConfig,
        MySQLSessionConfig,
        MySQLSSLConfig,
    )

    # MariaDB aliases (MariaDB is fully compatible with MySQL driver)
    MariaDB = MySQL
    MariaDBConnectionConfig = MySQLConnectionConfig
    MariaDBConnRetryConfig = MySQLConnRetryConfig
    MariaDBOpRetryConfig = MySQLOpRetryConfig
    MariaDBPersistent = MySQLPersistent
    MariaDBPoolConfig = MySQLPoolConfig
    MariaDBSessionConfig = MySQLSessionConfig
    MariaDBSSLConfig = MySQLSSLConfig
    clear_mariadb_settings_cache = clear_mysql_settings_cache
    get_mariadb_settings = get_mysql_settings

    __all__ += [
        "MySQL",
        "MySQLConnectionConfig",
        "MySQLConnRetryConfig",
        "MySQLOpRetryConfig",
        "MySQLPersistent",
        "MySQLPoolConfig",
        "MySQLSessionConfig",
        "MySQLSSLConfig",
        "clear_mysql_settings_cache",
        "get_mysql_settings",
        # MariaDB aliases
        "MariaDB",
        "MariaDBConnectionConfig",
        "MariaDBConnRetryConfig",
        "MariaDBOpRetryConfig",
        "MariaDBPersistent",
        "MariaDBPoolConfig",
        "MariaDBSessionConfig",
        "MariaDBSSLConfig",
        "clear_mariadb_settings_cache",
        "get_mariadb_settings",
    ]
except ImportError:
    pass

try:
    from .core.persistent import OraclePersistent
    from .core.settings import clear_oracle_settings_cache, get_oracle_settings
    from .oracle import (
        Oracle,
        OracleConnectionConfig,
        OracleConnRetryConfig,
        OracleOpRetryConfig,
        OraclePoolConfig,
        OracleSessionConfig,
        OracleSSLConfig,
    )

    __all__ += [
        "Oracle",
        "OracleConnectionConfig",
        "OracleConnRetryConfig",
        "OracleOpRetryConfig",
        "OraclePersistent",
        "OraclePoolConfig",
        "OracleSessionConfig",
        "OracleSSLConfig",
        "clear_oracle_settings_cache",
        "get_oracle_settings",
    ]
except ImportError:
    pass

try:
    from .core.persistent import PostgreSQLPersistent
    from .core.settings import clear_postgresql_settings_cache, get_postgresql_settings
    from .postgresql import (
        PostgreSQL,
        PostgreSQLConnectionConfig,
        PostgreSQLConnRetryConfig,
        PostgreSQLOpRetryConfig,
        PostgreSQLPoolConfig,
        PostgreSQLSessionConfig,
        PostgreSQLSSLConfig,
    )

    __all__ += [
        "PostgreSQL",
        "PostgreSQLConnectionConfig",
        "PostgreSQLConnRetryConfig",
        "PostgreSQLOpRetryConfig",
        "PostgreSQLPersistent",
        "PostgreSQLPoolConfig",
        "PostgreSQLSessionConfig",
        "PostgreSQLSSLConfig",
        "clear_postgresql_settings_cache",
        "get_postgresql_settings",
    ]
except ImportError:
    pass

__all__ = tuple(__all__)
__title__ = "ddcDatabases"
__author__ = "Daniel Costa"
__email__ = "danieldcsta@gmail.com>"
__license__ = "MIT"
__copyright__ = "Copyright 2024-present DDC Softwares"
__version__ = version(__title__)

logging.getLogger(__name__).addHandler(logging.NullHandler())
