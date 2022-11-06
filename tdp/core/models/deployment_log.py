# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import enum

from sqlalchemy import JSON, Boolean, Column, DateTime, Enum, Integer, String
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.operation import NODE_NAME_MAX_LENGTH

from .state_enum import StateEnum


class FilterTypeEnum(str, enum.Enum):
    REGEX = "regex"
    GLOB = "glob"


class DeploymentLog(Base):
    __tablename__ = "deployment_log"

    id = Column(Integer, primary_key=True)
    sources = Column(JSON)
    targets = Column(JSON)
    filter_expression = Column(String(length=NODE_NAME_MAX_LENGTH * 5))
    filter_type = Column(Enum(FilterTypeEnum))
    start_time = Column(DateTime(timezone=False))
    end_time = Column(DateTime(timezone=False))
    state: Column = Column(Enum(StateEnum))
    using_dag = Column(Boolean, default=True)
    restart = Column(Boolean, default=False)

    operations = relationship(
        "OperationLog", back_populates="deployment", order_by="OperationLog.start_time"
    )
    services = relationship("ServiceLog", back_populates="deployment")
