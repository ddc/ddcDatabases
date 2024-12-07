# -*- coding: utf-8 -*-
import os
import pytest
from ddcDatabases import Sqlite
from tests.dal.test_model_dal import TestModelDal
from tests.data.base_data import db_filename
from tests.models.test_model import ModelTest


class TestSQLite:
    @classmethod
    def setup_class(cls):
        with Sqlite(db_filename).engine() as engine:
            ModelTest.__table__.create(engine)

    @classmethod
    def teardown_class(cls):
        os.remove(db_filename)

    @pytest.fixture(autouse=True, scope="class")
    def test_insert(self, sqlite_session, fake_test_data):
        sqlite_session.add(ModelTest(**fake_test_data))
        config_dal = TestModelDal(sqlite_session)
        config_id = fake_test_data["id"]
        results = config_dal.get(config_id)
        assert len(results) == 1

    def test_get(self, sqlite_session, fake_test_data):
        test_dal = TestModelDal(sqlite_session)
        config_id = fake_test_data["id"]
        results = test_dal.get(config_id)
        assert len(results) == 1

    def test_update_str(self, sqlite_session, fake_test_data):
        test_dal = TestModelDal(sqlite_session)
        _id = fake_test_data["id"]
        name = "Test_1"
        test_dal.update_name(name, _id)
        results = test_dal.get(_id)
        assert results[0]["name"] == name

    def test_update_bool(self, sqlite_session, fake_test_data):
        test_dal = TestModelDal(sqlite_session)
        _id = fake_test_data["id"]
        status = (True, False,)
        for st in status:
            test_dal.update_enabled(st, _id)
            results = test_dal.get(_id)
            assert results[0]["enabled"] is st
