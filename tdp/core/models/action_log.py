# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, DateTime, ForeignKey, Integer, LargeBinary, String
from sqlalchemy.orm import relationship

from tdp.core.component import NODE_NAME_MAX_LENGTH
from tdp.core.models.base import Base
from tdp.core.runner.executor import StateEnum


class ActionLog(Base):
    __tablename__ = "action_log"

    deployment_id = Column(Integer, ForeignKey("deployment_log.id"), primary_key=True)
    action = Column(String(length=NODE_NAME_MAX_LENGTH), primary_key=True)
    start = Column(DateTime)
    end = Column(DateTime)
    state = Column(String(length=StateEnum.max_length()))
    logs = Column(LargeBinary)
    deployment = relationship("DeploymentLog", back_populates="actions")
