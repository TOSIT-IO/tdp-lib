# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime, timedelta

import pytest
from sqlalchemy import Table, create_engine
from sqlalchemy.orm import sessionmaker

from .base import Base
from .deployment_log import DeploymentLog
from .operation_log import OperationLog
from .service_component_log import ServiceComponentLog

logger = logging.getLogger("tdp").getChild("test_db")


@pytest.fixture(scope="session")
def session_maker():
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    Base.metadata.create_all(engine)
    session_maker = sessionmaker(bind=engine)

    yield session_maker

    Base.metadata.drop_all(engine)
    engine.dispose()


@pytest.fixture(autouse=True)
def clear_tables(session_maker: sessionmaker):
    table: Table
    with session_maker() as session:
        for table in reversed(Base.metadata.sorted_tables):
            session.execute(table.delete())
        session.commit()


def test_create_deployment_log(session_maker: sessionmaker):
    deployment_log = DeploymentLog(
        sources=["source1", "source2"],
        targets=["target1", "target2"],
        filter_expression=".*",
        filter_type="glob",
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow() + timedelta(0, 1),
        state="SUCCESS",
        deployment_type="Dag",
        restart=False,
    )
    service_component_log = ServiceComponentLog(
        deployment_id=deployment_log.id,
        service="service1",
        component="component1",
        version="1.0.0",
    )
    operation_log = OperationLog(
        deployment_id=deployment_log.id,
        operation="start_target1",
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow() + timedelta(0, 1),
        state="Success",
        logs=b"operation log",
    )

    deployment_log.service_components.append(service_component_log)
    deployment_log.operations.append(operation_log)

    logger.info(deployment_log)
    logger.info(operation_log)
    logger.info(operation_log)

    with session_maker() as session:
        session.add(deployment_log)
        session.commit()

        result = session.get(DeploymentLog, deployment_log.id)

        logger.info(result)
        logger.info(result.operations)
        logger.info(result.service_components)

        assert result is not None
        assert result.sources == ["source1", "source2"]
        assert result.targets == ["target1", "target2"]
        assert result.filter_expression == ".*"
        assert result.filter_type == "glob"
        assert result.state == "Success"
        assert result.deployment_type == "Dag"
        assert result.restart is False

        assert len(result.service_components) == 1
        assert result.service_components[0].service == "service1"
        assert result.service_components[0].component == "component1"
        assert result.service_components[0].version == "1.0.0"

        assert len(result.operations) == 1
        assert result.operations[0].operation == "start_target1"
        assert result.operations[0].state == "Success"
        assert result.operations[0].logs == b"operation log"
