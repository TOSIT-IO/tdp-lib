# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from .base import Base
from .deployment_log import DeploymentLog, FilterTypeEnum
from .deployment_type_enum import DeploymentTypeEnum
from .operation_log import OperationLog
from .service_component_log import ServiceComponentLog
from .state_enum import StateEnum


def init_database(engine):
    Base.metadata.create_all(engine)
