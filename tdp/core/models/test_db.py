# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.core.models.base import Base
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.models.operation_log import OperationLog

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
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow(),
        state="SUCCESS",
    )
    operation = OperationLog(
        operation="start_hdfs",
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow(),
        state="SUCCESS",
        logs=b"log",
    )
    deployment.operations.append(operation)
    logger.info(deployment)
    logger.info(operation)
    with session_class() as session:
        session.add(deployment)
        session.commit()
        deployment = session.get(DeploymentLog, deployment.id)
        logger.info(deployment)
        logger.info(deployment.operations)
