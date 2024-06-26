# -*- coding: utf-8 -*-
from faker import Faker
from ddcDatabases import DBSqlite


database_engine = DBSqlite(":memory:").engine()


def _set_randoms():
    _faker = Faker(locale="en_US")
    return {
        "id": _faker.random_int(min=1, max=9999999),
        "name": _faker.uuid4(),
        "enable": _faker.pybool(),
    }


def get_fake_test_data():
    rand = _set_randoms()
    return rand
