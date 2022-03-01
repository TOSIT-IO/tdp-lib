from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from tdp.core.models.base import Base


class ServiceLog(Base):
    __tablename__ = "service_log"

    deployment_id = Column(Integer, ForeignKey("deployment_log.id"), primary_key=True)
    service_id = Column(Integer, ForeignKey("service.id"), primary_key=True)
    version = Column(String, nullable=False)
    deployment = relationship("DeploymentLog", back_populates="services")
    service = relationship("Service", back_populates="deployments")
