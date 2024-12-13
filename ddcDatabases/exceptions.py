# -*- encoding: utf-8 -*-
import sys
from datetime import datetime


class CustomBaseException(Exception):
    def __init__(self, msg):
        dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        sys.stderr.write(f"[{dt}]:[ERROR]:{repr(msg)}")


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
