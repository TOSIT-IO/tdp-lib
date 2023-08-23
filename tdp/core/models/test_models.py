# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from tdp.core.models.component_version_log import ComponentVersionLog
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.models.operation_log import OperationLog

logger = logging.getLogger("tdp").getChild("test_db")


def test_create_deployment_log(db_session: Session):
    deployment_log = DeploymentLog(
        sources=["source1", "source2"],
        targets=["target1", "target2"],
        filter_expression=".*",
        filter_type="glob",
        hosts=["host1", "host2"],
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow() + timedelta(0, 1),
        state="SUCCESS",
        deployment_type="Dag",
        restart=False,
    )
    component_version_log = ComponentVersionLog(
        deployment_id=deployment_log.id,
        service="service1",
        component="component1",
        host=None,
        version="1.0.0",
    )
    operation_log = OperationLog(
        operation_order=1,
        deployment_id=deployment_log.id,
        operation="start_target1",
        host="host1",
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow() + timedelta(0, 1),
        state="Success",
        logs=b"operation log",
    )

    deployment_log.component_version.append(component_version_log)
    deployment_log.operations.append(operation_log)

    logger.info(deployment_log)
    logger.info(operation_log)
    logger.info(component_version_log)

    db_session.add(deployment_log)
    db_session.commit()

    result = db_session.get(DeploymentLog, deployment_log.id)

    logger.info(result)
    assert result is not None
    assert result.sources == ["source1", "source2"]
    assert result.targets == ["target1", "target2"]
    assert result.filter_expression == ".*"
    assert result.filter_type == "glob"
    assert result.hosts == ["host1", "host2"]
    assert result.state == "Success"
    assert result.deployment_type == "Dag"
    assert result.restart is False

    logger.info(result.operations)
    assert len(result.component_version) == 1
    assert result.component_version[0].service == "service1"
    assert result.component_version[0].component == "component1"
    assert result.component_version[0].version == "1.0.0"

    logger.info(result.component_version)
    assert len(result.operations) == 1
    assert result.operations[0].operation_order == 1
    assert result.operations[0].operation == "start_target1"
    assert result.operations[0].host == "host1"
    assert result.operations[0].state == "Success"
    assert result.operations[0].logs == b"operation log"
