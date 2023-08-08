# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest
from sqlalchemy import Column, Integer, String

from tdp.core.models.base import Base, keyvalgen


class ExampleClass(Base):
    __tablename__ = "test"

    id = Column(Integer, primary_key=True)
    name = Column(String)


@pytest.fixture
def test_instance():
    instance = ExampleClass(id=1, name="TestName")
    return instance


def test_keyvalgen(test_instance):
    gen = keyvalgen(test_instance)
    attrs = dict(gen)
    assert attrs == {"id": 1, "name": "TestName"}


def test_custom_base_repr(test_instance):
    repr_str = repr(test_instance)
    assert repr_str == "ExampleClass(id=1, name=TestName)"
