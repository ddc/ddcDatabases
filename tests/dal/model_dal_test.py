import sqlalchemy as sa
from ddcDatabases import DBUtils
from ddcDatabases.exceptions import DBFetchAllException
from tests.models.test_models import ModelTest


class ModelDalTest:
    """Data Abstraction Layer"""

    def __init__(self, db_session):
        self.columns = list(ModelTest.__table__.columns)
        self.db_utils = DBUtils(db_session)

    def update_name(self, name: str, test_id: int):
        stmt = sa.update(ModelTest).where(ModelTest.id == test_id).values(name=name)
        self.db_utils.execute(stmt)

    def update_enabled(self, status: bool, test_id: int):
        stmt = sa.update(ModelTest).where(ModelTest.id == test_id).values(enabled=status)
        self.db_utils.execute(stmt)

    def get(self, test_id: int):
        try:
            stmt = sa.select(*self.columns).where(ModelTest.id == test_id)
            results = self.db_utils.fetchall(stmt)
            return results
        except DBFetchAllException:
            return None
