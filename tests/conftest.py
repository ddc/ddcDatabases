# -*- coding: utf-8 -*-
import pytest
from sqlalchemy.pool import StaticPool
from ddcDatabases import Sqlite
from tests.data.base_data import get_fake_test_data, sqlite_filename


@pytest.fixture(name="sqlite_session", scope="session")
def sqlite_session():
    extra_engine_args = {"poolclass": StaticPool}
    with Sqlite(
        filepath=sqlite_filename,
        extra_engine_args=extra_engine_args,
    ) as session:
        yield session


@pytest.fixture(name="fake_test_data", scope="session")
def fake_test_data(sqlite_session):
    fdata = get_fake_test_data()
    yield fdata
