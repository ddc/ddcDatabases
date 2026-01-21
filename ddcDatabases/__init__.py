import logging
from importlib.metadata import version
from typing import Literal, NamedTuple
from ddcDatabases.db_utils import DBUtils, DBUtilsAsync, RetryConfig
from ddcDatabases.persistent import close_all_persistent_connections, PersistentConnectionConfig
from ddcDatabases.sqlite import Sqlite

# Conditional imports based on available dependencies
try:
    from .mongodb import MongoDB
    from .persistent import MongoDBPersistent
except ImportError:
    MongoDB = None
    MongoDBPersistent = None

try:
    from .mssql import MSSQL
    from .persistent import MSSQLPersistent
except ImportError:
    MSSQL = None
    MSSQLPersistent = None

try:
    from .mysql import MySQL
    from .persistent import MySQLPersistent
except ImportError:
    MySQL = None
    MySQLPersistent = None

try:
    from .oracle import Oracle
    from .persistent import OraclePersistent
except ImportError:
    Oracle = None
    OraclePersistent = None

try:
    from .postgresql import PostgreSQL
    from .persistent import PostgreSQLPersistent
except ImportError:
    PostgreSQL = None
    PostgreSQLPersistent = None


# Build __all__ dynamically based on successfully imported classes
__all__ = [
    "DBUtils",
    "DBUtilsAsync",
    "Sqlite",
    "RetryConfig",
    "PersistentConnectionConfig",
    "close_all_persistent_connections",
]

if MongoDB is not None:
    __all__.append("MongoDB")
if MongoDBPersistent is not None:
    __all__.append("MongoDBPersistent")
if MSSQL is not None:
    __all__.append("MSSQL")
if MSSQLPersistent is not None:
    __all__.append("MSSQLPersistent")
if MySQL is not None:
    __all__.append("MySQL")
if MySQLPersistent is not None:
    __all__.append("MySQLPersistent")
if Oracle is not None:
    __all__.append("Oracle")
if OraclePersistent is not None:
    __all__.append("OraclePersistent")
if PostgreSQL is not None:
    __all__.append("PostgreSQL")
if PostgreSQLPersistent is not None:
    __all__.append("PostgreSQLPersistent")

__all__ = tuple(__all__)


__title__ = "ddcDatabases"
__author__ = "Daniel Costa"
__email__ = "danieldcsta@gmail.com>"
__license__ = "MIT"
__copyright__ = "Copyright 2024-present ddc"
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
