# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from enum import Enum


class StateEnum(str, Enum):
    """State enum.

    Attributes:
        SUCCESS: Success state.
        FAILURE: Failure state.
        PENDING: Pending state.
    """

    SUCCESS = "Success"
    FAILURE = "Failure"
    PENDING = "Pending"

    @classmethod
    def has_value(cls, value: str) -> bool:
        """Check if value is a valid StateEnum value.

        Args:
            value: Value to check.

        Returns:
            True if value is a valid StateEnum value, False otherwise.
        """
        return isinstance(value, StateEnum) or value in (
            v.value for v in cls.__members__.values()
        )
