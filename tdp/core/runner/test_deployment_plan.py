# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.core.models.deployment_type_enum import DeploymentTypeEnum
from tdp.core.operation import Operation

from .deployment_plan import DeploymentPlan


def test_deployment_plan_from_operations():
    operations = [Operation("mock_start"), Operation("mock_init")]

    deployment_plan = DeploymentPlan.from_operations(operations)

    assert (
        deployment_plan.deployment_args["deployment_type"]
        == DeploymentTypeEnum.OPERATIONS
    )
    assert len(deployment_plan.operations) == 2
    assert any(filter(lambda op: op.name == "mock_start", deployment_plan.operations))


def test_deployment_plan_from_dag(dag):
    deployment_plan = DeploymentPlan.from_dag(dag, ["mock_start"])

    assert deployment_plan.deployment_args["deployment_type"] == DeploymentTypeEnum.DAG
    assert len(deployment_plan.operations) == 6
    assert any(filter(lambda op: op.name == "mock_start", deployment_plan.operations))


def test_deployment_plan_filter(dag):
    deployment_plan = DeploymentPlan.from_dag(
        dag, targets=["mock_init"], filter_expression="*_install"
    )

    assert all(
        filter(
            lambda operation: operation.name.endswith("_install"),  # type: ignore
            deployment_plan.operations,
        )
    ), "Filter expression should have left only install operations from dag"


def test_deployment_plan_restart(dag):
    deployment_plan = DeploymentPlan.from_dag(
        dag,
        targets=["mock_init"],
        restart=True,
    )

    assert any(
        filter(
            lambda operation: "_restart" in operation.name,  # type: ignore
            deployment_plan.operations,
        )
    ), "A restart operation should be present"
    assert not any(
        filter(
            lambda operation: "_start" in operation.name,  # type: ignore
            deployment_plan.operations,
        )
    ), "The restart flag should have removed every start operations from dag"
