from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from tdp.core.models.base import Base


class Service(Base):
    __tablename__ = "service"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    deployments = relationship("ServiceLog", back_populates="service")
