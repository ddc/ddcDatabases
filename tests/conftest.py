# -*- coding: utf-8 -*-
import pytest
from ddcDatabases import Sqlite
from tests.data.base_data import db_filename, get_fake_test_data


@pytest.fixture(name="sqlite_db_session", scope="session")
def sqlite_db_session():
    with Sqlite(db_filename) as session:
        yield session


@pytest.fixture(name="fake_test_data", scope="session")
def fake_test_data(sqlite_db_session):
    fdata = get_fake_test_data()
    yield fdata
