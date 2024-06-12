# -*- coding: utf-8 -*-
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session
from tests.data.base_data import database_engine, get_fake_test_data
from tests.models.sqlite_model import ModelTest


@pytest.fixture(name="db_session")
def db_session():
    with Session(database_engine) as session:
        yield session


@pytest.fixture
def fake_test_data(db_session):
    # init
    fdata = get_fake_test_data()
    yield fdata
    # teardown
    db_session.execute(sa.delete(ModelTest))
