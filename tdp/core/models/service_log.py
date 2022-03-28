from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from tdp.core.models.base import Base
from tdp.core.repository.repository import VERSION_MAX_LENGTH
from tdp.core.service_manager import SERVICE_NAME_MAX_LENGTH


class ServiceLog(Base):
    __tablename__ = "service_log"

    deployment_id = Column(Integer, ForeignKey("deployment_log.id"), primary_key=True)
    service = Column(String(length=SERVICE_NAME_MAX_LENGTH), primary_key=True)
    version = Column(String(length=VERSION_MAX_LENGTH), nullable=False)
    deployment = relationship("DeploymentLog", back_populates="services")
