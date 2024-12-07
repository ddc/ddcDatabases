# -*- coding: utf-8 -*-
from faker import Faker


db_filename = "test.db"


def _set_randoms():
    _faker = Faker(locale="en_US")
    return {
        "id": _faker.random_int(min=1, max=9999999),
        "name": _faker.uuid4(),
        "enabled": _faker.pybool(),
    }


def get_fake_test_data():
    rand = _set_randoms()
    return rand
