# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from enum import Enum


class StateEnum(Enum):
    SUCCESS = "Success"
    FAILURE = "Failure"
    PENDING = "Pending"

    @classmethod
    def has_value(cls, value):
        return isinstance(value, StateEnum) or value in (
            v.value for v in cls.__members__.values()
        )

    @classmethod
    def max_length(cls):
        return max(len(state.value) for state in list(StateEnum))


class Executor(ABC):
    """An Executor is an object able to run an operation."""

    @abstractmethod
    def execute(self, operation):
        """Executes an operation

        Args:
            operation (str): Operation name

        Returns:
            Tuple[StateEnum, bytes]: Whether an operation is a success as well as its logs in UTF-8 bytes.
        """
        pass
