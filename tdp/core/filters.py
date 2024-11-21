# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import fnmatch
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from typing import Type

from tdp.core.models.enums import FilterTypeEnum
from tdp.core.operation import Operation


class FilterStrategy(ABC):
    """Interface for filter strategies."""

    @abstractmethod
    def apply_filter(
        self, operations: Iterable[Operation], expression: str
    ) -> list[Operation]:
        """Apply the filter strategy to the operations."""
        pass


class RegexFilterStrategy(FilterStrategy):
    """Filter strategy that uses regular expressions."""

    def apply_filter(self, operations, expression):
        compiled_regex = re.compile(expression)
        return [o for o in operations if compiled_regex.match(o.name.name)]


class GlobFilterStrategy(FilterStrategy):
    """Filter strategy that uses glob patterns."""

    def apply_filter(self, operations, expression):
        return [o for o in operations if fnmatch.fnmatch(o.name.name, expression)]


class FilterFactory:
    """Factory for creating filter strategies."""

    _strategies: dict[FilterTypeEnum, Type[FilterStrategy]] = {
        FilterTypeEnum.REGEX: RegexFilterStrategy,
        FilterTypeEnum.GLOB: GlobFilterStrategy,
    }

    @staticmethod
    def create_filter(
        filter_type: FilterTypeEnum, filter_expression: str
    ) -> Callable[[Iterable[Operation]], list[Operation]]:
        """Create a filter function based on the filter type and expression."""
        strategy_class = FilterFactory._strategies.get(filter_type)
        if not strategy_class:
            raise ValueError(f"Unsupported filter type: {filter_type}")
        strategy = strategy_class()
        return lambda operations: strategy.apply_filter(operations, filter_expression)
