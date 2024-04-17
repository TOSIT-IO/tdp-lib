# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime, timedelta

import pytest
from sqlalchemy.engine import Engine

from tdp.conftest import create_session
from tdp.core.models import DeploymentModel, OperationModel, SCHStatusLogModel

logger = logging.getLogger(__name__)


# TODO: add some status logs
@pytest.mark.parametrize("db_engine", [True], indirect=True)
def test_create_deployment(db_engine: Engine):
    deployment = DeploymentModel(
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
        state="SUCCESS",
        deployment_type="Dag",
    )
    component_version_log = SCHStatusLogModel(
        deployment_id=deployment.id,
        service="service1",
        component="component1",
        host=None,
        running_version="1.0.0",
    )
    operation_rec = OperationModel(
        operation_order=1,
        deployment_id=deployment.id,
        operation="start_target1",
        host="host1",
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow() + timedelta(0, 1),
        state="Success",
        logs=b"operation log",
    )

    deployment.operations.append(operation_rec)

    logger.info(deployment)
    logger.info(operation_rec)
    logger.info(component_version_log)

    with create_session(db_engine) as session:
        session.add(deployment)
        session.commit()

        result = session.get(DeploymentModel, deployment.id)

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
        assert result.state == "Success"
        assert result.deployment_type == "Dag"

        logger.info(result.operations)
        assert len(result.operations) == 1
        assert result.operations[0].operation_order == 1
        assert result.operations[0].operation == "start_target1"
        assert result.operations[0].host == "host1"
        assert result.operations[0].state == "Success"
        assert result.operations[0].logs == b"operation log"
