# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from tdp.core.entities.deployment_entity import (
    Deployment_entity,
    transform_to_deployment_entity,
)
from tdp.core.models.deployment_model import DeploymentModel
from tdp.core.models.enums import DeploymentStateEnum, DeploymentTypeEnum


class TestDeploymentEntity:
    def test_deployment_entity(self):
        deployemnt_entity = Deployment_entity(
            id=1,
            options={"test": "option for test"},
            start_time=datetime(2024, 5, 15),
            end_time=datetime(2024, 5, 16),
            state=DeploymentStateEnum.RUNNING,
            deployment_type=DeploymentTypeEnum.DAG,
            operations=[],
        )

        deployemnt_model = DeploymentModel(
            id=1,
            options={"test": "option for test"},
            start_time=datetime(2024, 5, 15),
            end_time=datetime(2024, 5, 16),
            state=DeploymentStateEnum.RUNNING,
            deployment_type=DeploymentTypeEnum.DAG,
            operations=[],
        )

        assert deployemnt_entity.transform_to_deployment_model().to_dict(
            filter_out=["_sa_instance_state"]
        ) == deployemnt_model.to_dict(filter_out=["_sa_instance_state"])

    def test_transform_to_deployment_entity(self):
        deployemnt_entity = Deployment_entity(
            id=1,
            options={"test": "option for test"},
            start_time=datetime(2024, 5, 15),
            end_time=datetime(2024, 5, 16),
            state=DeploymentStateEnum.RUNNING,
            deployment_type=DeploymentTypeEnum.DAG,
            operations=[],
        )

        deployemnt_model = DeploymentModel(
            id=1,
            options={"test": "option for test"},
            start_time=datetime(2024, 5, 15),
            end_time=datetime(2024, 5, 16),
            state=DeploymentStateEnum.RUNNING,
            deployment_type=DeploymentTypeEnum.DAG,
            operations=[],
        )

        assert (
            deployemnt_entity.__dict__
            == transform_to_deployment_entity(deployemnt_model).__dict__
        )
