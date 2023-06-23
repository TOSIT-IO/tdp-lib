# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, LargeBinary, String
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.operation import OPERATION_NAME_MAX_LENGTH

from .state_enum import OperationStateEnum


class OperationLog(Base):
    """Operation log model.

    Hold past and current operation information linked to a deployment.

    Attributes:
        deployment_id (int): Deployment log id.
        operation_order (int): Operation order.
        operation (str): Operation name.
        start_time (datetime): Operation start time.
        end_time (datetime): Operation end time.
        state (OperationStateEnum): Operation state.
        logs (bytes): Operation logs.
    """

    __tablename__ = "operation_log"

    deployment_id = Column(Integer, ForeignKey("deployment_log.id"), primary_key=True)
    operation_order = Column(Integer, primary_key=True)
    operation = Column(String(length=OPERATION_NAME_MAX_LENGTH))
    start_time = Column(DateTime(timezone=False))
    end_time = Column(DateTime(timezone=False))
    state = Column(Enum(OperationStateEnum))
    logs = Column(LargeBinary)

    deployment = relationship("DeploymentLog", back_populates="operations")
