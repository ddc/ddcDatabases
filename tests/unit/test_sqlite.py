# -*- coding: utf-8 -*-
from tests.dal.model_dal_test import ModelDalTest
from tests.models.model_test import ModelTest


class TestSQLite:
    @classmethod
    def setup_class(cls):
        """ setup_class """
        pass

    @classmethod
    def teardown_class(cls):
        """ teardown_class """
        pass

    def test_sqlite(self, sqlite_session, fake_test_data):
        sqlite_engine = sqlite_session.bind
        ModelTest.__table__.create(sqlite_engine)
        sqlite_session.add(ModelTest(**fake_test_data))
        config_dal = ModelDalTest(sqlite_session)
        config_id = fake_test_data["id"]
        results = config_dal.get(config_id)
        assert len(results) == 1

        # test_get
        test_dal = ModelDalTest(sqlite_session)
        config_id = fake_test_data["id"]
        results = test_dal.get(config_id)
        assert len(results) == 1

        # test_update_str
        test_dal = ModelDalTest(sqlite_session)
        _id = fake_test_data["id"]
        name = "Test_1"
        test_dal.update_name(name, _id)
        results = test_dal.get(_id)
        assert results[0]["name"] == name

        # test_update_bool
        test_dal = ModelDalTest(sqlite_session)
        _id = fake_test_data["id"]
        status = (True, False,)
        for st in status:
            test_dal.update_enabled(st, _id)
            results = test_dal.get(_id)
            assert results[0]["enabled"] is st
