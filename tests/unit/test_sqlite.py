# -*- coding: utf-8 -*-
import pytest
from tests.dal.sqlite_dal import SqliteDal
from tests.data.base_data import database_engine
from tests.models.sqlite_model import ModelTest


class TestConfigDal:
    @classmethod
    def setup_class(cls):
        ModelTest.__table__.create(database_engine)

    @classmethod
    def teardown_class(cls):
        ModelTest.__table__.drop(database_engine)

    @pytest.fixture(autouse=True)
    def test_insert(self, db_session, fake_test_data):
        config_dal = SqliteDal(db_session)
        config_id = fake_test_data["id"]
        db_session.add(ModelTest(**fake_test_data))
        results = config_dal.get(config_id)
        assert len(results) == 1

    def test_get(self, db_session, fake_test_data):
        test_dal = SqliteDal(db_session)
        config_id = fake_test_data["id"]
        results = test_dal.get(config_id)
        assert len(results) == 1

    def test_update_name(self, db_session, fake_test_data):
        test_dal = SqliteDal(db_session)
        _id = fake_test_data["id"]
        name = "Test_1"
        test_dal.update_name(name, _id)
        results = test_dal.get(_id)
        assert results[0]["name"] == name

    def test_update_enable(self, db_session, fake_test_data):
        test_dal = SqliteDal(db_session)
        _id = fake_test_data["id"]
        status = (True, False,)
        for st in status:
            test_dal.update_enable(st, _id)
            results = test_dal.get(_id)
            assert results[0]["enable"] is st
