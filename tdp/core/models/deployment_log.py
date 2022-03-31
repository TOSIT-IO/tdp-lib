# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import JSON, Column, DateTime, Integer, String
from sqlalchemy.orm import relationship

from tdp.core.component import NODE_NAME_MAX_LENGTH
from tdp.core.models.base import Base
from tdp.core.runner.executor import StateEnum


class DeploymentLog(Base):
    __tablename__ = "deployment_log"

    id = Column(Integer, primary_key=True)
    sources = Column(JSON)
    targets = Column(JSON)
    filter = Column(String(length=NODE_NAME_MAX_LENGTH))
    start = Column(DateTime)
    end = Column(DateTime)
    state = Column(String(length=StateEnum.max_length()))
    actions = relationship(
        "ActionLog", back_populates="deployment", order_by="ActionLog.start"
    )
    services = relationship("ServiceLog", back_populates="deployment")
