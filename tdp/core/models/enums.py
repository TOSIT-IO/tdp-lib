# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.core.utils import BaseEnum

_PLANNED_STATE = "Planned"
_RUNNING_STATE = "Running"
_PENDING_STATE = "Pending"
_SUCCESS_STATE = "Success"
_FAILURE_STATE = "Failure"
_HELD_STATE = "Held"


class OperationStateEnum(BaseEnum):
    PLANNED = _PLANNED_STATE
    RUNNING = _RUNNING_STATE
    PENDING = _PENDING_STATE
    SUCCESS = _SUCCESS_STATE
    FAILURE = _FAILURE_STATE
    HELD = _HELD_STATE


class DeploymentStateEnum(BaseEnum):
    PLANNED = _PLANNED_STATE
    RUNNING = _RUNNING_STATE
    SUCCESS = _SUCCESS_STATE
    FAILURE = _FAILURE_STATE


class DeploymentTypeEnum(BaseEnum):
    DAG = "Dag"
    OPERATIONS = "Operations"
    RESUME = "Resume"
    RECONFIGURE = "Reconfigure"
    CUSTOM = "Custom"


class FilterTypeEnum(BaseEnum):
    REGEX = "regex"
    GLOB = "glob"


class SCHStatusLogSourceEnum(BaseEnum):
    """Source of the status log."""

    DEPLOYMENT = "Deployment"
    FORCED = "Forced"
    STALE = "Stale"
    MANUAL = "Manual"
