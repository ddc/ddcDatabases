# -*- coding: utf-8 -*-
import pytest
from ddcDatabases import Sqlite
from tests.data.base_data import db_filename, get_fake_test_data
from pytest_postgresql import factories
from pytest_postgresql.janitor import DatabaseJanitor
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from tests.models.test_model import ModelTest


@pytest.fixture(name="sqlite_session", scope="session")
def sqlite_session():
    with Sqlite(db_filename) as session:
        yield session


@pytest.fixture(name="fake_test_data", scope="session")
def fake_test_data(sqlite_session):
    fdata = get_fake_test_data()
    yield fdata


@pytest.fixture(scope="module")
def create_test_data():
    names = ("Winifred", "Sarah", "Mary")
    test_objs = []
    for idx, name in zip(range(3), names):
        test_objs.append(ModelTest(id=idx, name=name, enabled=True))
    return test_objs
