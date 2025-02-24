import logging
from importlib.metadata import version
from typing import Literal, NamedTuple
from .db_utils import DBUtils, DBUtilsAsync
from .mssql import MSSQL
from .mysql import MySQL
from .oracle import Oracle
from .postgresql import PostgreSQL
from .sqlite import Sqlite


__all__ = (
    "DBUtils",
    "DBUtilsAsync",
    "MSSQL",
    "MySQL",
    "Oracle",
    "PostgreSQL",
    "Sqlite",
)


__title__ = "ddcDatabases"
__author__ = "Daniel Costa"
__email__ = "danieldcsta@gmail.com>"
__license__ = "MIT"
__copyright__ = "Copyright 2024-present ddc"
_req_python_version = (3, 10, 0)


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
