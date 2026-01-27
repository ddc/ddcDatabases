from ddcDatabases.core.operations import DBUtils, DBUtilsAsync
from ddcDatabases.core.persistent import PersistentConnectionConfig, close_all_persistent_connections
from ddcDatabases.sqlite import (
    Sqlite,
    SqliteConnRetryConfig,
    SqliteOpRetryConfig,
    SqliteSessionConfig,
)
from importlib.metadata import version
import logging
from typing import Literal, NamedTuple

__all__ = [
    "DBUtils",
    "DBUtilsAsync",
    "PersistentConnectionConfig",
    "Sqlite",
    "SqliteConnRetryConfig",
    "SqliteOpRetryConfig",
    "SqliteSessionConfig",
    "close_all_persistent_connections",
]

# Conditional imports based on available dependencies
try:
    from .core.persistent import MongoDBPersistent
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
    ]
except ImportError:
    pass

try:
    from .core.persistent import MSSQLPersistent
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
    ]
except ImportError:
    pass

try:
    from .core.persistent import MySQLPersistent
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

    __all__ += [
        "MySQL",
        "MySQLConnectionConfig",
        "MySQLConnRetryConfig",
        "MySQLOpRetryConfig",
        "MySQLPersistent",
        "MySQLPoolConfig",
        "MySQLSessionConfig",
        "MySQLSSLConfig",
        # MariaDB aliases
        "MariaDB",
        "MariaDBConnectionConfig",
        "MariaDBConnRetryConfig",
        "MariaDBOpRetryConfig",
        "MariaDBPersistent",
        "MariaDBPoolConfig",
        "MariaDBSessionConfig",
        "MariaDBSSLConfig",
    ]
except ImportError:
    pass

try:
    from .core.persistent import OraclePersistent
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
    ]
except ImportError:
    pass

try:
    from .core.persistent import PostgreSQLPersistent
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
    ]
except ImportError:
    pass

__all__ = tuple(__all__)
__title__ = "ddcDatabases"
__author__ = "Daniel Costa"
__email__ = "danieldcsta@gmail.com>"
__license__ = "MIT"
__copyright__ = "Copyright 2024-present DDC Softwares"
_req_python_version = (3, 12, 0)


try:
    _version = tuple(int(x) for x in version(__title__).split("."))
except ModuleNotFoundError:
    _version = (0, 0, 0)


class VersionInfo(NamedTuple):
    major: int
    minor: int
    micro: int
    releaselevel: Literal["alpha", "beta", "candidate", "final"]
    serial: int


__version__ = _version
__version_info__: VersionInfo = VersionInfo(
    major=__version__[0],
    minor=__version__[1],
    micro=__version__[2],
    releaselevel="final",
    serial=0,
)
__req_python_version__: VersionInfo = VersionInfo(
    major=_req_python_version[0],
    minor=_req_python_version[1],
    micro=_req_python_version[2],
    releaselevel="final",
    serial=0,
)

logging.getLogger(__name__).addHandler(logging.NullHandler())

del (
    logging,
    NamedTuple,
    Literal,
    VersionInfo,
    version,
    _version,
    _req_python_version,
)
