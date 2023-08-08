# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.core.models.base import Base
from tdp.core.models.component_version_log import ComponentVersionLog
from tdp.core.models.deployment_log import (
    DeploymentLog,
    DeploymentTypeEnum,
    FilterTypeEnum,
    NoOperationMatchError,
    NothingToReconfigureError,
    NothingToResumeError,
)
from tdp.core.models.operation_log import OperationLog
from tdp.core.models.stale_component import StaleComponent
from tdp.core.models.state_enum import DeploymentStateEnum, OperationStateEnum


def init_database(engine):
    Base.metadata.create_all(engine)
