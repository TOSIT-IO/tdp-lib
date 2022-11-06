# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from .base import Base
from .deployment_log import DeploymentLog, FilterTypeEnum
from .operation_log import OperationLog
from .service_log import ServiceLog
from .state_enum import StateEnum


def init_database(engine):
    Base.metadata.create_all(engine)
