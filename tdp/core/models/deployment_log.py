# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from enum import Enum

from sqlalchemy import JSON, Boolean, Column, DateTime, Integer, String
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.operation import NODE_NAME_MAX_LENGTH
from tdp.core.runner.executor import StateEnum


class FilterTypeEnum(str, Enum):
    REGEX = "regex"
    GLOB = "glob"


class DeploymentLog(Base):
    __tablename__ = "deployment_log"

    id = Column(Integer, primary_key=True)
    sources = Column(JSON)
    targets = Column(JSON)
    filter_expression = Column(String(length=NODE_NAME_MAX_LENGTH))
    filter_type = Column(String(length=5))
    start_time = Column(DateTime(timezone=False))
    end_time = Column(DateTime(timezone=False))
    state = Column(String(length=StateEnum.max_length()))
    using_dag = Column(Boolean, default=True)

    operations = relationship(
        "OperationLog", back_populates="deployment", order_by="OperationLog.start_time"
    )
    services = relationship("ServiceLog", back_populates="deployment")
