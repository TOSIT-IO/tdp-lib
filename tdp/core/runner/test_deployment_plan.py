# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.core.models import DeploymentLog, DeploymentTypeEnum, OperationLog, StateEnum
from tdp.core.operation import Operation

from .deployment_plan import DeploymentPlan, NothingToResumeError


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


def failed_deployment_log(deployment_plan, index_to_fail):

    deployment_log = DeploymentLog(
        id=1, state=StateEnum.FAILURE, **deployment_plan.deployment_args
    )
    deployment_log.operations = [
        OperationLog(
            deployment_id=1,
            operation=operation.name,
            start_time=-1,
            end_time=-1,
            state=StateEnum.SUCCESS,
            logs=b"",
        )
        for operation in deployment_plan.operations[:index_to_fail]
    ]
    deployment_log.operations.append(
        OperationLog(
            deployment_id=1,
            operation=deployment_plan.operations[index_to_fail].name,
            start_time=-1,
            end_time=-1,
            state=StateEnum.FAILURE,
            logs=b"",
        )
    )
    return deployment_log


def success_deployment_log(deployment_plan):
    deployment_log = DeploymentLog(
        id=1, state=StateEnum.SUCCESS, **deployment_plan.deployment_args
    )
    deployment_log.operations = [
        OperationLog(
            deployment_id=1,
            operation=operation.name,
            start_time=-1,
            end_time=-1,
            state=StateEnum.SUCCESS,
            logs=b"",
        )
        for operation in deployment_plan.operations
    ]
    return deployment_log


def test_deployment_plan_resume_from_dag(dag):
    deployment_plan = DeploymentPlan.from_dag(
        dag,
        targets=["mock_init"],
    )
    index_to_fail = 2
    deployment_log = failed_deployment_log(deployment_plan, index_to_fail)

    resume_plan = DeploymentPlan.from_failed_deployment(dag, deployment_log)
    assert resume_plan.deployment_args["deployment_type"] == DeploymentTypeEnum.RESUME
    assert len(deployment_plan.operations) - index_to_fail == len(
        resume_plan.operations
    )
    assert set(deployment_plan.operations) >= set(resume_plan.operations)


def test_deployment_plan_resume_from_dag_with_filter(dag):
    deployment_plan = DeploymentPlan.from_dag(
        dag, targets=["mock_init"], filter_expression="mock_node_*"
    )
    index_to_fail = 3
    deployment_log = failed_deployment_log(deployment_plan, index_to_fail)

    resume_plan = DeploymentPlan.from_failed_deployment(dag, deployment_log)
    assert resume_plan.deployment_args["deployment_type"] == DeploymentTypeEnum.RESUME
    assert len(deployment_plan.operations) - index_to_fail == len(
        resume_plan.operations
    )
    assert set(deployment_plan.operations) >= set(resume_plan.operations)


def test_deployment_plan_resume_with_success_deployment(dag):
    deployment_plan = DeploymentPlan.from_dag(
        dag,
        targets=["mock_init"],
    )
    deployment_log = success_deployment_log(deployment_plan)
    with pytest.raises(NothingToResumeError):
        DeploymentPlan.from_failed_deployment(dag, deployment_log)
