# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

_PLANNED_STATE = "Planned"
_RUNNING_STATE = "Running"
_PENDING_STATE = "Pending"
_SUCCESS_STATE = "Success"
_FAILURE_STATE = "Failure"
_HELD_STATE = "Held"


class _StateEnum(str, Enum):
    """State enum.

    Attributes:
        SUCCESS: Success state.
        FAILURE: Failure state.
        PENDING: Pending state.
    """

    @classmethod
    def has_value(cls: "_StateEnum", value: str) -> bool:
        """Check if value is a valid StateEnum value.

        Args:
            value: Value to check.

        Returns:
            True if value is a valid StateEnum value, False otherwise.
        """
        return isinstance(value, _StateEnum) or value in (
            v.value for v in cls.__members__.values()
        )


class OperationStateEnum(_StateEnum):
    PLANNED = _PLANNED_STATE
    RUNNING = _RUNNING_STATE
    PENDING = _PENDING_STATE
    SUCCESS = _SUCCESS_STATE
    FAILURE = _FAILURE_STATE
    HELD = _HELD_STATE


class DeploymentStateEnum(_StateEnum):
    PLANNED = _PLANNED_STATE
    RUNNING = _RUNNING_STATE
    SUCCESS = _SUCCESS_STATE
    FAILURE = _FAILURE_STATE
