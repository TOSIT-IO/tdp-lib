# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Union

_PLANNED_STATE = "Planned"
_RUNNING_STATE = "Running"
_PENDING_STATE = "Pending"
_SUCCESS_STATE = "Success"
_FAILURE_STATE = "Failure"
_HELD_STATE = "Held"


class _StateEnum(str, Enum):
    @classmethod
    def has_value(cls: Union["OperationStateEnum", "DeploymentStateEnum"], value: str):
        """Check if value is a valid StateEnum value.

        Args:
            value (str): Value to check.

        Returns:
            bool: True if value is a valid StateEnum value, False otherwise.
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
    PENDING = _PENDING_STATE  # TODO: remove this state (should be replaced by RUNNING)
