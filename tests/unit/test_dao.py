# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from tdp.core.models import (
    DeploymentModel,
    OperationModel,
)
from tdp.core.models.base_model import BaseModel
from tdp.core.models.enums import DeploymentStateEnum, OperationStateEnum
from tdp.dao import Dao


def assert_equal_values_in_model(model1: Any, model2: Any) -> bool:
    """SQLAlchemy asserts that two identical objects of type DeclarativeBase parent of the BaseModel class,
    which is used in TDP as pattern for the table models, are identical if they are compared in the same session,
    but different if compared in two different sessions.

    This function therefore transforms the tables into dictionaries and by parsing the coulumns compares their values.
    """
    if isinstance(model1, BaseModel) and isinstance(model2, BaseModel):
        return model1.to_dict() == model2.to_dict()
    else:
        return False


def test_get_deployment(db_session: Session, db_engine: Engine):
    db_session.add(
        DeploymentModel(
            id=1,
            state=DeploymentStateEnum.RUNNING,
        )
    )
    db_session.commit()

    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            dao.get_deployment(1),
            DeploymentModel(
                id=1,
                state=DeploymentStateEnum.RUNNING,
            ),
        )


def test_get_planned_deployment(db_session: Session, db_engine: Engine):
    db_session.add(
        DeploymentModel(
            id=1,
            state=DeploymentStateEnum.PLANNED,
        )
    )
    db_session.commit()

    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            dao.get_planned_deployment(),
            DeploymentModel(
                id=1,
                state=DeploymentStateEnum.PLANNED,
            ),
        )


def test_get_last_deployment(db_session: Session, db_engine: Engine):
    db_session.add(
        DeploymentModel(
            id=2,
            state=DeploymentStateEnum.FAILURE,
        )
    )
    db_session.add(
        DeploymentModel(
            id=3,
            state=DeploymentStateEnum.SUCCESS,
        )
    )
    db_session.commit()

    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            dao.get_last_deployment(),
            DeploymentModel(
                id=3,
                state=DeploymentStateEnum.SUCCESS,
            ),
        )


def test_get_deployments(db_session: Session, db_engine: Engine):
    db_session.add(
        DeploymentModel(
            id=1,
            state=DeploymentStateEnum.SUCCESS,
        )
    )
    db_session.add(
        DeploymentModel(
            id=2,
            state=DeploymentStateEnum.PLANNED,
        )
    )
    db_session.commit()

    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            list(dao.get_last_deployments())[0],
            DeploymentModel(id=1, state=DeploymentStateEnum.SUCCESS),
        )
        assert assert_equal_values_in_model(
            list(dao.get_last_deployments())[1],
            DeploymentModel(id=2, state=DeploymentStateEnum.PLANNED),
        )


def test_operation(db_session: Session, db_engine: Engine):
    db_session.add(DeploymentModel(id=1, state=DeploymentStateEnum.SUCCESS))
    db_session.add(
        OperationModel(
            deployment_id=1,
            operation_order=1,
            operation="test_operation",
            state=OperationStateEnum.SUCCESS,
        )
    )
    db_session.commit()

    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            dao.get_operations_by_name(
                deployment_id=1, operation_name="test_operation"
            )[0],
            OperationModel(
                deployment_id=1,
                operation_order=1,
                operation="test_operation",
                state=OperationStateEnum.SUCCESS,
            ),
        )
