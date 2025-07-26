import logging
from importlib.metadata import version
from typing import Literal, NamedTuple
from ddcDatabases.db_utils import DBUtils, DBUtilsAsync
from ddcDatabases.sqlite import Sqlite


# Conditional imports based on available dependencies
try:
    from .mongodb import MongoDB
except ImportError:
    MongoDB = None

try:
    from .mssql import MSSQL
except ImportError:
    MSSQL = None

try:
    from .mysql import MySQL
except ImportError:
    MySQL = None

try:
    from .oracle import Oracle
except ImportError:
    Oracle = None

try:
    from .postgresql import PostgreSQL
except ImportError:
    PostgreSQL = None


# Build __all__ dynamically based on successfully imported classes
__all__ = ["DBUtils", "DBUtilsAsync", "Sqlite"]

if MongoDB is not None:
    __all__.append("MongoDB")
if MSSQL is not None:
    __all__.append("MSSQL")
if MySQL is not None:
    __all__.append("MySQL")
if Oracle is not None:
    __all__.append("Oracle")
if PostgreSQL is not None:
    __all__.append("PostgreSQL")

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
