# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import enum

from sqlalchemy import JSON, Boolean, Column, DateTime, Enum, Integer, String
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.operation import OPERATION_NAME_MAX_LENGTH

from .state_enum import StateEnum


class DeploymentTypeEnum(str, enum.Enum):
    """Deployment type enum.

    Attributes:
        DAG (str): Dag deployment type.
        OPERATIONS (str): Operations deployment type.
        RESUME (str): Resume deployment type.
        RECONFIGURE (str): Reconfigure deployment type.
    """

    DAG = "Dag"
    OPERATIONS = "Operations"
    RESUME = "Resume"
    RECONFIGURE = "Reconfigure"


class FilterTypeEnum(str, enum.Enum):
    """Filter type enum.

    Attributes:
        REGEX (str): Regex filter type.
        GLOB (str): Glob filter type.
    """

    REGEX = "regex"
    GLOB = "glob"


class DeploymentLog(Base):
    """Deployment log model.

    Attributes:
        id (int): Deployment log id.
        sources (list): List of source nodes, in the case of Dag deployment type.
        targets (list): List of target nodes, in the case of Dag deployment type.
        filter_expression (str): Filter expression.
        filter_type (enum): Filter type (regex or glob).
        start_time (datetime): Deployment start time.
        end_time (datetime): Deployment end time.
        state (enum): Deployment state (Success, Failure or Pending).
        deployment_type (str): Deployment type (Dag, Operations, Resume or Reconfigure).
        restart (bool): Restart flag.
    """

    __tablename__ = "deployment_log"

    id = Column(Integer, primary_key=True)
    sources = Column(JSON)
    targets = Column(JSON)
    filter_expression = Column(String(length=OPERATION_NAME_MAX_LENGTH * 5))
    filter_type = Column(Enum(FilterTypeEnum))
    start_time = Column(DateTime(timezone=False))
    end_time = Column(DateTime(timezone=False))
    state: Column = Column(Enum(StateEnum))
    deployment_type: Column = Column(Enum(DeploymentTypeEnum))
    restart = Column(Boolean, default=False)

    operations = relationship(
        "OperationLog", back_populates="deployment", order_by="OperationLog.start_time"
    )
    service_components = relationship(
        "ServiceComponentLog", back_populates="deployment"
    )
