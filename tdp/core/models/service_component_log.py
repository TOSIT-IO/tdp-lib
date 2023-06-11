# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.operation import COMPONENT_NAME_MAX_LENGTH, SERVICE_NAME_MAX_LENGTH
from tdp.core.repository.repository import VERSION_MAX_LENGTH


class ServiceComponentLog(Base):
    """Component log model.

    Hold what component version is deployed.

    Attributes:
        id (int): Service component log id.
        deployment_id (int): Deployment log id.
        service (str): Service name.
        component (str): Component name.
        version (str): Component version.
    """

    __tablename__ = "service_component_log"

    id = Column(Integer, primary_key=True)
    deployment_id = Column(Integer, ForeignKey("deployment_log.id"), nullable=False)
    service = Column(String(length=SERVICE_NAME_MAX_LENGTH), nullable=False)
    component = Column(String(length=COMPONENT_NAME_MAX_LENGTH), nullable=True)
    version = Column(String(length=VERSION_MAX_LENGTH), nullable=False)

    deployment = relationship("DeploymentLog", back_populates="service_components")

    __table_args__ = (UniqueConstraint("deployment_id", "service", "component"),)
