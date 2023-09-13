# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from tdp.core.models import DeploymentLog, OperationLog, SCHStatusLog

logger = logging.getLogger("tdp").getChild("test_db")


# TODO: add some status logs
def test_create_deployment_log(db_session: Session):
    deployment_log = DeploymentLog(
        options={
            "sources": ["source1", "source2"],
            "targets": ["target1", "target2"],
            "filter_expression": ".*",
            "filter_type": "glob",
            "hosts": ["host1", "host2"],
            "restart": False,
        },
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow() + timedelta(0, 1),
        status="SUCCESS",
        deployment_type="Dag",
    )
    component_version_log = SCHStatusLog(
        deployment_id=deployment_log.id,
        service="service1",
        component="component1",
        host=None,
        running_version="1.0.0",
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

    deployment_log.operations.append(operation_log)

    logger.info(deployment_log)
    logger.info(operation_log)
    logger.info(component_version_log)

    db_session.add(deployment_log)
    db_session.commit()

    result = db_session.get(DeploymentLog, deployment_log.id)

    logger.info(result)
    assert result is not None
    assert result.options == {
        "sources": ["source1", "source2"],
        "targets": ["target1", "target2"],
        "filter_expression": ".*",
        "filter_type": "glob",
        "hosts": ["host1", "host2"],
        "restart": False,
    }
    assert result.status == "Success"
    assert result.deployment_type == "Dag"

    logger.info(result.operations)
    assert len(result.operations) == 1
    assert result.operations[0].operation_order == 1
    assert result.operations[0].operation == "start_target1"
    assert result.operations[0].host == "host1"
    assert result.operations[0].state == "Success"
    assert result.operations[0].logs == b"operation log"
