# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, DateTime, ForeignKey, Integer, LargeBinary, String
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.operation import NODE_NAME_MAX_LENGTH
from tdp.core.runner.executor import StateEnum


class OperationLog(Base):
    __tablename__ = "operation_log"

    deployment_id = Column(Integer, ForeignKey("deployment_log.id"), primary_key=True)
    operation = Column(String(length=NODE_NAME_MAX_LENGTH), primary_key=True)
    start = Column(DateTime(timezone=False))
    end = Column(DateTime(timezone=False))
    state = Column(String(length=StateEnum.max_length()))
    logs = Column(LargeBinary)
    deployment = relationship("DeploymentLog", back_populates="operations")
