# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.core.models.action_log import ActionLog
from tdp.core.models.base import Base
from tdp.core.models.deployment_log import DeploymentLog

logger = logging.getLogger("tdp").getChild("test_db")


@pytest.fixture(scope="session")
def session_class():
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    Base.metadata.create_all(engine)
    session_class = sessionmaker(bind=engine)
    return session_class


def test_add_object(session_class):
    deployment = DeploymentLog(
        targets=["init_hdfs"],
        start=datetime.utcnow(),
        end=datetime.utcnow(),
        state="SUCCESS",
    )
    action = ActionLog(
        action="start_hdfs",
        start=datetime.utcnow(),
        end=datetime.utcnow(),
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
