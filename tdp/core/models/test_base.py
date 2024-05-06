# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest
from sqlalchemy import Column, Integer, String

from tdp.core.models.base_model import BaseModel


class ExampleClass(BaseModel):
    __tablename__ = "test"

    id = Column(Integer, primary_key=True)
    name = Column(String(10))


@pytest.fixture
def test_instance() -> ExampleClass:
    instance = ExampleClass(id=1, name="TestName")
    return instance


def test_custom_base_to_dict(test_instance: ExampleClass):
    dict_repr = test_instance.to_dict(format=False)
    assert dict_repr == {"id": 1, "name": "TestName"}


def test_custom_base_repr(test_instance: ExampleClass):
    repr_str = repr(test_instance)
    assert repr_str == "ExampleClass(id=1, name=TestName)"
