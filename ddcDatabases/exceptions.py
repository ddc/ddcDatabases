# -*- encoding: utf-8 -*-
import sys


def get_exception(e) -> str:
    """
    Get exception
    :param e: exception string
    :return: str
    """

    module = e.__class__.__module__
    if module is None or module == str.__class__.__module__:
        module_and_exception = f"[{e.__class__.__name__}]:[{e}]"
    else:
        module_and_exception = f"[{module}.{e.__class__.__name__}]:[{e}]"
    return module_and_exception.replace("\r\n", " ").replace("\n", " ")


class CustomBaseException(Exception):
    def __init__(self, msg):
        sys.stderr.write(get_exception(msg))


class DBAddException(CustomBaseException):
    pass


class DBExecuteException(CustomBaseException):
    pass


class DBFetchAllException(CustomBaseException):
    pass


class DBFetchOneException(CustomBaseException):
    pass


class DBFetchValueException(CustomBaseException):
    pass
