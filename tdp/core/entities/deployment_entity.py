# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from tdp.core.entities.operation_entity import (
    Operation_entity,
    transform_to_operation_entity,
)
from tdp.core.models.deployment_model import DeploymentModel
from tdp.core.models.enums import DeploymentStateEnum, DeploymentTypeEnum


@dataclass
class Deployment_entity:
    id: int
    operations: list[Operation_entity]
    options: Optional[dict] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    state: Optional[DeploymentStateEnum] = None
    deployment_type: Optional[DeploymentTypeEnum] = None

    def transform_to_deployment_model(self) -> DeploymentModel:
        return DeploymentModel(
            id=self.id,
            options=self.options,
            start_time=self.start_time,
            end_time=self.end_time,
            state=self.state,
            deployment_type=self.deployment_type,
            operations=[
                Operation_entity.transform_to_operation_model(operation)
                for operation in self.operations
            ],
        )


def transform_to_deployment_entity(
    deployment_model: DeploymentModel,
) -> Deployment_entity:
    return Deployment_entity(
        id=deployment_model.id,
        options=deployment_model.options,
        start_time=deployment_model.start_time,
        end_time=deployment_model.end_time,
        state=deployment_model.state,
        deployment_type=deployment_model.deployment_type,
        operations=[
            transform_to_operation_entity(operation)
            for operation in deployment_model.operations
        ],
    )
