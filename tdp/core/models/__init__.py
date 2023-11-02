# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

from sqlalchemy.engine.row import Row

from tdp.core.models.base_model import BaseModel
from tdp.core.models.deployment_model import (
    DeploymentModel,
    DeploymentTypeEnum,
    FilterTypeEnum,
    NoOperationMatchError,
    NothingToReconfigureError,
    NothingToResumeError,
)
from tdp.core.models.operation_log import OperationLog
from tdp.core.models.sch_status_log import SCHStatusLog, SCHStatusLogSourceEnum
from tdp.core.models.state_enum import DeploymentStateEnum, OperationStateEnum

ServiceComponentHostStatus = tuple[
    str,  # service
    Optional[str],  # component
    Optional[str],  # host
    Optional[str],  # running_version
    Optional[str],  # configured_version
    Optional[int],  # to_config
    Optional[int],  # to_restart
]

SCHStatusRow = Row[ServiceComponentHostStatus]


def init_database(engine):
    BaseModel.metadata.create_all(engine)
