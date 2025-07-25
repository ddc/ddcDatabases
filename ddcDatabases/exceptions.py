import sys
from datetime import datetime, timezone


class CustomBaseException(Exception):
    def __init__(self, msg):
        now = datetime.now(timezone.utc)
        dt = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        utc_offset = now.strftime("%z")
        sys.stderr.write(f"[{dt}{utc_offset}]:[ERROR]:{repr(msg)}\n")
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
