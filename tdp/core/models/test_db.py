from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest

from tdp.core.models import Base
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.models.action_log import ActionLog


import logging
from datetime import datetime

logger = logging.getLogger("tdp").getChild("test_db")


@pytest.fixture(scope="session")
def session_class():
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    Base.metadata.create_all(engine)
    session_class = sessionmaker(bind=engine)
    return session_class


def test_add_object(session_class):
    deployment = DeploymentLog(
        target="init_hdfs", start=datetime.now(), end=datetime.now(), state="SUCCESS"
    )
    action = ActionLog(
        action="start_hdfs",
        start=datetime.now(),
        end=datetime.now(),
        state="SUCCESS",
        logs=b"log",
    )
    deployment.actions.append(action)
    logger.info(deployment)
    logger.info(action)
    with session_class() as session:
        session.add(deployment)
        session.commit()
        deployment = session.get(DeploymentLog, deployment.id)
        logger.info(deployment)
        logger.info(deployment.actions)
