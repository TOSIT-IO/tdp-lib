from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.orm import relationship
from . import Base


class DeploymentLog(Base):
    __tablename__ = "deployment_log"

    id = Column(Integer, primary_key=True)
    target = Column(String)
    filter = Column(String)
    start = Column(DateTime)
    end = Column(DateTime)
    state = Column(String)
    actions = relationship("ActionLog", back_populates="deployment")
