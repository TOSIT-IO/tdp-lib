from sqlalchemy import BLOB, Column, DateTime, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from tdp.core.models.base import Base


class ActionLog(Base):
    __tablename__ = "action_log"

    deployment_id = Column(Integer, ForeignKey("deployment_log.id"), primary_key=True)
    action = Column(String, primary_key=True)
    start = Column(DateTime)
    end = Column(DateTime)
    state = Column(String)
    logs = Column(BLOB)
    deployment = relationship("DeploymentLog", back_populates="actions")
