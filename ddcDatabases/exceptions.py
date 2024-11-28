# -*- encoding: utf-8 -*-
import sys


class CustomBaseException(Exception):
    def __init__(self, msg):
        sys.stderr.write(repr(msg))


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
