# -*- encoding: utf-8 -*-
from .exceptions import (
    DBAddException,
    DBExecuteException,
    DBFetchAllException,
    DBFetchOneException,
    DBFetchValueException
)


class DBUtils:
    def __init__(self, session):
        self.session = session

    def add(self, stmt):
        try:
            self.session.add(stmt)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBAddException(e)

    def execute(self, stmt):
        try:
            self.session.execute(stmt)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBExecuteException(e)

    def fetchall(self, stmt):
        cursor = None
        try:
            cursor = self.session.execute(stmt)
            return cursor.mappings().all()
        except Exception as e:
            self.session.rollback()
            raise DBFetchAllException(e)
        finally:
            cursor.close() if cursor is not None else None

    def fetchone(self, stmt):
        cursor = None
        try:
            cursor = self.session.execute(stmt)
            return cursor.mappings().first()
        except Exception as e:
            self.session.rollback()
            raise DBFetchOneException(e)
        finally:
            cursor.close() if cursor is not None else None

    def fetch_value(self, stmt):
        cursor = None
        try:
            cursor = self.session.execute(stmt)
            return cursor.first()[0]
        except Exception as e:
            self.session.rollback()
            raise DBFetchValueException(e)
        finally:
            cursor.close() if cursor is not None else None


class DBUtilsAsync:
    def __init__(self, session):
        self.session = session

    async def add(self, stmt):
        try:
            self.session.add(stmt)
            await self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DBAddException(e)

    async def execute(self, stmt):
        try:
            await self.session.execute(stmt)
            await self.session.commit()
        except Exception as e:
            await self.session.rollback()
            raise DBExecuteException(e)

    async def fetchall(self, stmt):
        cursor = None
        try:
            cursor = await self.session.execute(stmt)
            return cursor.mappings().all()
        except Exception as e:
            await self.session.rollback()
            raise DBFetchAllException(e)
        finally:
            cursor.close() if cursor is not None else None

    async def fetchone(self, stmt):
        cursor = None
        try:
            cursor = await self.session.execute(stmt)
            return cursor.mappings().first()
        except Exception as e:
            await self.session.rollback()
            raise DBFetchOneException(e)
        finally:
            cursor.close() if cursor is not None else None

    async def fetch_value(self, stmt):
        cursor = None
        try:
            cursor = await self.session.execute(stmt)
            return cursor.first()[0]
        except Exception as e:
            self.session.rollback()
            raise DBFetchValueException(e)
        finally:
            cursor.close() if cursor is not None else None
