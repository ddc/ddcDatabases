# -*- coding: utf-8 -*-
import sys
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import RowMapping
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session
from .exceptions import (
    DBDeleteAllDataException,
    DBExecuteException,
    DBFetchAllException,
    DBFetchValueException,
    DBInsertBulkException,
    DBInsertSingleException,
)


class TestConnections:
    def __init__(
        self,
        sync_session: Session = None,
        async_session: AsyncSession = None,
        host_url: URL = "",
    ):
        self.dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        self.sync_session = sync_session
        self.async_session = async_session
        self.host_url = host_url

    def test_connection_sync(self) -> None:
        try:
            self.sync_session.execute(sa.text("SELECT 1"))
            sys.stdout.write(
                f"[{self.dt}]:[INFO]:Connection to database successful | "
                f"{self.host_url}\n"
            )
        except Exception as e:
            self.sync_session.close()
            sys.stderr.write(
                f"[{self.dt}]:[ERROR]:Connection to datatabse failed | "
                f"{self.host_url} | "
                f"{repr(e)}\n"
            )
            raise

    async def test_connection_async(self) -> None:
        try:
            await self.async_session.execute(sa.text("SELECT 1"))
            sys.stdout.write(
                f"[{self.dt}]:[INFO]:Connection to database successful | "
                f"{self.host_url}\n"
            )
        except Exception as e:
            await self.async_session.close()
            sys.stderr.write(
                f"[{self.dt}]:[ERROR]:Connection to datatabse failed | "
                f"{self.host_url} | "
                f"{repr(e)}\n"
            )
            raise


class DBUtils:
    def __init__(self, session):
        self.session = session

    def fetchall(self, stmt) -> list[RowMapping]:
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

    async def fetchall(self, stmt) -> list[RowMapping]:
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
