# -*- encoding: utf-8 -*-
from sqlalchemy.engine.result import MappingResult
from sqlalchemy.ext.asyncio import AsyncMappingResult
from .exceptions import (
    DBDeleteAllDataException,
    DBExecuteException,
    DBFetchAllException,
    DBFetchValueException,
    DBInsertBulkException,
    DBInsertSingleException
)


class DBUtils:
    def __init__(self, session):
        self.session = session

    def fetchall(self, stmt) -> MappingResult:
        cursor = None
        try:
            cursor = self.session.execute(stmt)
            return cursor.mappings().all()
        except Exception as e:
            self.session.rollback()
            raise DBFetchAllException(e)
        finally:
            cursor.close() if cursor is not None else None

    def fetchvalue(self, stmt) -> str | None:
        cursor = None
        try:
            cursor = self.session.execute(stmt)
            result = cursor.fetchone()
            return str(result[0]) if result is not None else None
        except Exception as e:
            self.session.rollback()
            raise DBFetchValueException(e)
        finally:
            cursor.close() if cursor is not None else None

    def insert(self, stmt) -> None:
        try:
            self.session.add(stmt)
        except Exception as e:
            self.session.rollback()
            raise DBInsertSingleException(e)
        finally:
            self.session.commit()

    def insertbulk(self, model, list_data: list[dict]) -> None:
        try:
            self.session.bulk_insert_mappings(model, list_data)
        except Exception as e:
            self.session.rollback()
            raise DBInsertBulkException(e)
        finally:
            self.session.commit()

    def deleteall(self, model) -> None:
        try:
            self.session.query(model).delete()
        except Exception as e:
            self.session.rollback()
            raise DBDeleteAllDataException(e)
        finally:
            self.session.commit()

    def execute(self, stmt) -> None:
        try:
            self.session.execute(stmt)
        except Exception as e:
            self.session.rollback()
            raise DBExecuteException(e)
        finally:
            self.session.commit()


class DBUtilsAsync:
    def __init__(self, session):
        self.session = session

    async def fetchall(self, stmt) -> AsyncMappingResult:
        cursor = None
        try:
            cursor = await self.session.execute(stmt)
            return cursor.mappings().all()
        except Exception as e:
            await self.session.rollback()
            raise DBFetchAllException(e)
        finally:
            cursor.close() if cursor is not None else None

    async def fetchvalue(self, stmt) -> str | None:
        cursor = None
        try:
            cursor = await self.session.execute(stmt)
            result = cursor.fetchone()
            return str(result[0]) if result is not None else None
        except Exception as e:
            await self.session.rollback()
            raise DBFetchValueException(e)
        finally:
            cursor.close() if cursor is not None else None

    async def insert(self, stmt) -> None:
        try:
            self.session.add(stmt)
        except Exception as e:
            await self.session.rollback()
            raise DBInsertSingleException(e)
        finally:
            await self.session.commit()

    async def insertbulk(self, model, list_data: list[dict]) -> None:
        try:
            self.session.bulk_insert_mappings(model, list_data)
        except Exception as e:
            await self.session.rollback()
            raise DBInsertBulkException(e)
        finally:
            await self.session.commit()

    async def deleteall(self, model) -> None:
        try:
            await self.session.query(model).delete()
        except Exception as e:
            await self.session.rollback()
            raise DBDeleteAllDataException(e)
        finally:
            await self.session.commit()

    async def execute(self, stmt) -> None:
        try:
            await self.session.execute(stmt)
        except Exception as e:
            await self.session.rollback()
            raise DBExecuteException(e)
        finally:
            await self.session.commit()
