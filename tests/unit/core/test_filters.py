# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

import pytest

from tdp.core.entities.operation import Operation, OperationName
from tdp.core.filters import FilterFactory, GlobFilterStrategy, RegexFilterStrategy
from tdp.core.models.enums import FilterTypeEnum


@dataclass(frozen=True)
class MockOperation(Operation):
    pass


@pytest.fixture
def operations():
    return [
        MockOperation(OperationName.from_str("service1_component1_config")),
        MockOperation(OperationName.from_str("service1_component1_start")),
        MockOperation(OperationName.from_str("service1_component2_config")),
        MockOperation(OperationName.from_str("service2_component1_start")),
    ]


def test_regex_filter_strategy(operations):
    regex_filter = RegexFilterStrategy()
    filtered_operations = regex_filter.apply_filter(operations, r"^.+_config$")
    assert len(filtered_operations) == 2
    assert all(op.name.name.endswith("_config") for op in filtered_operations)


def test_glob_filter_strategy(operations):
    glob_filter = GlobFilterStrategy()
    filtered_operations = glob_filter.apply_filter(operations, "*config")
    assert len(filtered_operations) == 2
    assert all(op.name.name.endswith("_config") for op in filtered_operations)


def test_filter_factory_with_regex(operations):
    filter_func = FilterFactory.create_filter(FilterTypeEnum.REGEX, r"^.+_config$")
    filtered_operations = filter_func(operations)
    assert len(filtered_operations) == 2


def test_filter_factory_with_glob(operations):
    filter_func = FilterFactory.create_filter(FilterTypeEnum.GLOB, "*config")
    filtered_operations = filter_func(operations)
    assert len(filtered_operations) == 2


def test_filter_factory_with_unsupported_type():
    with pytest.raises(ValueError):
        FilterFactory.create_filter("unsupported_type", "test*")  # type: ignore
