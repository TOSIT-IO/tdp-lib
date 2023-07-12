# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.core.models import (
    ComponentVersionLog,
    DeploymentLog,
    DeploymentStateEnum,
    DeploymentTypeEnum,
    OperationLog,
    OperationStateEnum,
)
from tdp.core.operation import Operation

from .deployment_plan import DeploymentPlan, NothingToRestartError, NothingToResumeError


def test_deployment_plan_from_operations():
    operations = [Operation("mock_start"), Operation("mock_init")]

    deployment_plan = DeploymentPlan.from_operations(operations)

    assert (
        deployment_plan.deployment_log.deployment_type == DeploymentTypeEnum.OPERATIONS
    )
    assert len(deployment_plan.operations) == 2
    assert any(filter(lambda op: op.name == "mock_start", deployment_plan.operations))


def test_deployment_plan_from_dag(dag):
    deployment_plan = DeploymentPlan.from_dag(dag, ["mock_start"])

    assert deployment_plan.deployment_log.deployment_type == DeploymentTypeEnum.DAG
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


def failed_deployment_log(deployment_plan: DeploymentPlan, index_to_fail):
    deployment_log = deployment_plan.deployment_log
    deployment_log.state = DeploymentStateEnum.FAILURE
    for operation in deployment_log.operations:
        if operation.operation_order < index_to_fail:
            operation.state = OperationStateEnum.SUCCESS
        elif operation.operation_order == index_to_fail:
            operation.state = OperationStateEnum.FAILURE
        else:
            operation.state = OperationStateEnum.HELD
    return deployment_log


def success_deployment_log(deployment_plan):
    deployment_log = DeploymentLog(
        id=1,
        state=DeploymentStateEnum.SUCCESS,
        deployment_type=deployment_plan.deployment_log.deployment_type,
        targets=deployment_plan.deployment_log.targets,
        sources=deployment_plan.deployment_log.sources,
        filter_expression=deployment_plan.deployment_log.filter_expression,
        filter_type=deployment_plan.deployment_log.filter_type,
        restart=deployment_plan.deployment_log.restart,
    )
    deployment_log.operations = [
        OperationLog(
            deployment_id=1,
            operation=operation.name,
            start_time=-1,
            end_time=-1,
            state=OperationStateEnum.SUCCESS,
            logs=b"",
        )
        for operation in deployment_plan.operations
    ]
    return deployment_log


def test_deployment_plan_resume_from_dag(dag, minimal_collections):
    deployment_plan = DeploymentPlan.from_dag(
        dag,
        targets=["mock_init"],
    )
    index_to_fail = 2
    deployment_log = failed_deployment_log(deployment_plan, index_to_fail)

    resume_plan = DeploymentPlan.from_failed_deployment(
        minimal_collections, deployment_log
    )
    assert resume_plan.deployment_log.deployment_type == DeploymentTypeEnum.RESUME
    # index starts at 1
    assert len(deployment_plan.operations) - index_to_fail + 1 == len(
        resume_plan.operations
    )
    assert len(deployment_plan.operations) >= len(resume_plan.operations)


def test_deployment_plan_resume_with_success_deployment(dag, minimal_collections):
    deployment_plan = DeploymentPlan.from_dag(
        dag,
        targets=["mock_init"],
    )
    deployment_log = success_deployment_log(deployment_plan)
    with pytest.raises(NothingToResumeError):
        DeploymentPlan.from_failed_deployment(minimal_collections, deployment_log)


def test_deployment_plan_reconfigure_nothing_to_restart(dag, cluster_variables):
    component_version_log = ComponentVersionLog(
        service="mock",
        component="node",
        version=cluster_variables["mock"].version,
    )
    with pytest.raises(NothingToRestartError):
        _deployment_plan = DeploymentPlan.from_reconfigure(
            dag,
            cluster_variables,
            [component_version_log],
        )


def test_deployment_plan_reconfigure(dag, reconfigurable_cluster_variables):
    (
        cluster_variables,
        component_version_deployed,
    ) = reconfigurable_cluster_variables
    deployment_plan = DeploymentPlan.from_reconfigure(
        dag, cluster_variables, component_version_deployed
    )

    assert len(deployment_plan.operations) == 4
    assert (
        deployment_plan.deployment_log.deployment_type == DeploymentTypeEnum.RECONFIGURE
    )
