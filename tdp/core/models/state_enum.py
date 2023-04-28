# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from enum import Enum


class StateEnum(str, Enum):
    """State enum.

    Attributes:
        SUCCESS (str): Success state.
        FAILURE (str): Failure state.
        PENDING (str): Pending state.
    """

    SUCCESS = "Success"
    FAILURE = "Failure"
    PENDING = "Pending"

    @classmethod
    def has_value(cls, value):
        """Check if value is a valid StateEnum value.

        Args:
            value (str): Value to check.

        Returns:
            bool: True if value is a valid StateEnum value, False otherwise.
        """
        return isinstance(value, StateEnum) or value in (
            v.value for v in cls.__members__.values()
        )
