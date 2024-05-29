# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.conftest import create_session
from tdp.core.entities.deployment_entity import Deployment_entity
from tdp.core.entities.operation_entity import Operation_entity
from tdp.core.models.deployment_model import DeploymentModel
from tdp.core.models.enums import DeploymentStateEnum, OperationStateEnum
from tdp.core.models.operation_model import OperationModel
from tdp.dao import Dao


@pytest.mark.parametrize("db_engine", [True], indirect=True)
class TestDao:
    def test_get_planned_deployment_dao1(self, db_engine):
        with create_session(db_engine) as session:
            deployment_content = DeploymentModel(
                id=1, state=DeploymentStateEnum.PLANNED
            )
            session.add(deployment_content)
            session.commit()
        with Dao(db_engine) as dao:
            assert (
                dao.get_planned_deployment_dao().__dict__
                == Deployment_entity(
                    id=1, state=DeploymentStateEnum.PLANNED, operations=[]
                ).__dict__
            )

    def test_get_planned_deployment_dao2(self, db_engine):
        with Dao(db_engine) as dao:
            deployment_content = DeploymentModel(
                id=1, state=DeploymentStateEnum.PLANNED
            )
            dao.session.add(deployment_content)
            dao.session.commit()
            assert (
                dao.get_planned_deployment_dao().__dict__
                == Deployment_entity(
                    id=1, state=DeploymentStateEnum.PLANNED, operations=[]
                ).__dict__
            )

    def test_get_operation_dao1(self, db_engine):
        with Dao(db_engine) as dao:
            deployment_content = DeploymentModel(id=1)
            dao.session.add(deployment_content)
            operation_content = OperationModel(
                deployment_id=1,
                operation_order=1,
                operation="test_operation_dao",
                state=OperationStateEnum.RUNNING,
            )
            dao.session.add(operation_content)
            dao.session.commit()
            assert (
                dao.get_operation_dao(
                    deployment_id=1, operation_name="test_operation_dao"
                )[0].__dict__
                == Operation_entity(
                    deployment_id=1,
                    operation_order=1,
                    operation="test_operation_dao",
                    state=OperationStateEnum.RUNNING,
                ).__dict__
            )
