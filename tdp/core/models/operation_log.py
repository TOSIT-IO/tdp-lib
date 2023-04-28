# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, LargeBinary, String
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.operation import NODE_NAME_MAX_LENGTH

from .state_enum import StateEnum


class OperationLog(Base):
    """Operation log model.

    Attributes:
        deployment_id (int): Deployment log id.
        operation (str): Operation name.
        start_time (datetime): Operation start time.
        end_time (datetime): Operation end time.
        state (enum): Operation state (Success, Failure or Pending).
        logs (bytes): Operation logs.
    """

    __tablename__ = "operation_log"

    deployment_id = Column(Integer, ForeignKey("deployment_log.id"), primary_key=True)
    operation = Column(String(length=NODE_NAME_MAX_LENGTH), primary_key=True)
    start_time = Column(DateTime(timezone=False))
    end_time = Column(DateTime(timezone=False))
    state = Column(Enum(StateEnum))
    logs = Column(LargeBinary)

    deployment = relationship("DeploymentLog", back_populates="operations")
