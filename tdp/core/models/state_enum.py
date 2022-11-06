# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

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
