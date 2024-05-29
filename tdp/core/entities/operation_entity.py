# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from tdp.core.models.enums import OperationStateEnum
from tdp.core.models.operation_model import OperationModel


@dataclass
class Operation_entity:
    deployment_id: int
    operation_order: int
    operation: str
    state: OperationStateEnum
    host: Optional[str] = None
    extra_vars: Optional[list[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    logs: Optional[bytes] = None

    def transform_to_operation_model(self) -> OperationModel:
        return OperationModel(
            deployment_id=self.deployment_id,
            operation_order=self.operation_order,
            operation=self.operation,
            state=self.state,
            host=self.host,
            extra_vars=self.extra_vars,
            start_time=self.start_time,
            end_time=self.end_time,
            logs=self.logs,
        )


def transform_to_operation_entity(operation_model: OperationModel) -> Operation_entity:
    return Operation_entity(
        deployment_id=operation_model.deployment_id,
        operation_order=operation_model.operation_order,
        operation=operation_model.operation,
        state=operation_model.state,
        host=operation_model.host,
        extra_vars=operation_model.extra_vars,
        start_time=operation_model.start_time,
        end_time=operation_model.end_time,
        logs=operation_model.logs,
    )
