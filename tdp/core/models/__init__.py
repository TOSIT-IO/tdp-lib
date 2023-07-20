# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from .base import Base
from .deployment_log import DeploymentLog, DeploymentTypeEnum, FilterTypeEnum
from .operation_log import OperationLog
from .component_version_log import ComponentVersionLog
from .state_enum import DeploymentStateEnum, OperationStateEnum
from .stale_component import StaleComponent


def init_database(engine):
    Base.metadata.create_all(engine)
