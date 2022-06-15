# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import select

from tdp.core.models.base import Base
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.models.operation_log import OperationLog
from tdp.core.models.service_log import ServiceLog


def init_database(engine):
    Base.metadata.create_all(engine)
