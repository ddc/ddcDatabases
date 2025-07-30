import sys
from datetime import datetime, timezone
from typing import Any


class CustomBaseException(Exception):
    """Base exception with timestamp generation"""

    __slots__ = ('original_exception',)

    def __init__(self, msg: Any) -> None:
        self.original_exception = msg
        now = datetime.now(timezone.utc)
        dt = now.isoformat(timespec='milliseconds')
        sys.stderr.write(f"[{dt}]:[ERROR]:{repr(msg)}\n")
        raise msg


class DBFetchAllException(CustomBaseException):
    pass


class DBFetchValueException(CustomBaseException):
    pass


class DBInsertSingleException(CustomBaseException):
    pass


class DBInsertBulkException(CustomBaseException):
    pass


class DBDeleteAllDataException(CustomBaseException):
    pass


class DBExecuteException(CustomBaseException):
    pass
