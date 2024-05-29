# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

from tdp.core.entities.deployment_entity import (
    Operation_entity,
    transform_to_operation_entity,
)
from tdp.core.models.enums import OperationStateEnum
from tdp.core.models.operation_model import OperationModel


class TestOperationEntity:
    def test_deployment_entity(self):
        operation_entity = Operation_entity(
            deployment_id=1,
            operation_order=11,
            operation="test_operation",
            state=OperationStateEnum.PLANNED,
            host="master01",
            extra_vars=["python==3.9"],
            start_time=datetime(2024, 5, 15),
            end_time=datetime(2024, 5, 16),
            logs="test logs".encode("utf-8"),
        )

        operation_model = OperationModel(
            deployment_id=1,
            operation_order=11,
            operation="test_operation",
            state=OperationStateEnum.PLANNED,
            host="master01",
            extra_vars=["python==3.9"],
            start_time=datetime(2024, 5, 15),
            end_time=datetime(2024, 5, 16),
            logs="test logs".encode("utf-8"),
        )

        assert operation_entity.transform_to_operation_model().to_dict(
            filter_out=["_sa_instance_state"]
        ) == operation_model.to_dict(filter_out=["_sa_instance_state"])

    def test_transform_to_operation_entity(self):
        operation_entity = Operation_entity(
            deployment_id=1,
            operation_order=11,
            operation="test_operation",
            state=OperationStateEnum.PLANNED,
            host="master01",
            extra_vars=["python==3.9"],
            start_time=datetime(2024, 5, 15),
            end_time=datetime(2024, 5, 16),
            logs="test logs".encode("utf-8"),
        )

        operation_model = OperationModel(
            deployment_id=1,
            operation_order=11,
            operation="test_operation",
            state=OperationStateEnum.PLANNED,
            host="master01",
            extra_vars=["python==3.9"],
            start_time=datetime(2024, 5, 15),
            end_time=datetime(2024, 5, 16),
            logs="test logs".encode("utf-8"),
        )

        assert (
            operation_entity.__dict__
            == transform_to_operation_entity(operation_model).__dict__
        )
