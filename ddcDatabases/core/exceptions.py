import logging
from datetime import datetime, timezone
from typing import Any

_logger = logging.getLogger(__name__)
_logger.addHandler(logging.NullHandler())


class CustomBaseException(Exception):
    """Base exception with timestamp generation"""

    __slots__ = ('original_exception',)

    def __init__(self, msg: Any) -> None:
        self.original_exception = msg
        now = datetime.now(timezone.utc)
        dt = now.isoformat(timespec='milliseconds')
        _logger.error(f"[{dt}]:{repr(msg)}")
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
