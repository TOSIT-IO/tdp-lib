# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import enum

from sqlalchemy import JSON, Boolean, Column, DateTime, Enum, Integer, String
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.operation import OPERATION_NAME_MAX_LENGTH

from .state_enum import DeploymentStateEnum


class DeploymentTypeEnum(str, enum.Enum):
    """Deployment type enum.

    Attributes:
        DAG: Dag deployment type.
        OPERATIONS: Operations deployment type.
        RESUME: Resume deployment type.
        RECONFIGURE: Reconfigure deployment type.
    """

    DAG = "Dag"
    OPERATIONS = "Operations"
    RESUME = "Resume"
    RECONFIGURE = "Reconfigure"


class FilterTypeEnum(str, enum.Enum):
    """Filter type enum.

    Attributes:
        REGEX: Regex filter type.
        GLOB: Glob filter type.
    """

    REGEX = "regex"
    GLOB = "glob"


class DeploymentLog(Base):
    """Deployment log model.

    Hold past and current deployment information.

    Attributes:
        id (int): Deployment log id.
        sources (List[str]): List of source nodes, in the case of Dag deployment type.
        targets (List[str]): List of target nodes, in the case of Dag deployment type. List of operations, in the case of Run deployment type.
        filter_expression (str): Filter expression.
        filter_type (FilterTypeEnum): Filter type.
        start_time (datetime): Deployment start time.
        end_time (datetime): Deployment end time.
        state (DeploymentStateEnum): Deployment state.
        deployment_type (DeploymentTypeEnum): Deployment type.
        restart (bool): Restart flag.
    """

    __tablename__ = "deployment_log"

    id = Column(Integer, primary_key=True)
    sources = Column(JSON(none_as_null=True))
    targets = Column(JSON(none_as_null=True))
    filter_expression = Column(String(length=OPERATION_NAME_MAX_LENGTH * 5))
    filter_type = Column(Enum(FilterTypeEnum))
    start_time = Column(DateTime(timezone=False))
    end_time = Column(DateTime(timezone=False))
    state: Column = Column(Enum(DeploymentStateEnum))
    deployment_type: Column = Column(Enum(DeploymentTypeEnum))
    restart = Column(Boolean, default=False)

    operations = relationship(
        "OperationLog",
        back_populates="deployment",
        order_by="OperationLog.start_time",
        cascade="all, delete-orphan",
    )
    component_version = relationship(
        "ComponentVersionLog", back_populates="deployment"
    )
