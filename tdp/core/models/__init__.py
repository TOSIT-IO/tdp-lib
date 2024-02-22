# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Optional

from sqlalchemy.engine.row import Row

from tdp.core.models.base_model import BaseModel
from tdp.core.models.deployment_model import (
    DeploymentModel,
    NoOperationMatchError,
    NothingToReconfigureError,
    NothingToResumeError,
)
from tdp.core.models.operation_model import OperationModel
from tdp.core.models.sch_status_log_model import (
    SCHStatusLogModel,
    SCHStatusLogSourceEnum,
)

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
