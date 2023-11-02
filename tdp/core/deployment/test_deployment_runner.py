# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional

import pytest

from tdp.core.cluster_status import ClusterStatus
from tdp.core.deployment.deployment_runner import DeploymentRunner
from tdp.core.deployment.executor import Executor
from tdp.core.models import (
    DeploymentModel,
    DeploymentStateEnum,
    DeploymentTypeEnum,
    OperationStateEnum,
)

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.dag import Dag
    from tdp.core.variables import ClusterVariables


class MockExecutor(Executor):
    """Mock executor for testing purposes."""

    def execute(
        self,
        playbook: str,
        host: Optional[str] = None,
        extra_vars: Optional[Iterable[str]] = None,
    ):
        return OperationStateEnum.SUCCESS, f"{playbook} LOG SUCCESS".encode("utf-8")


class FailingExecutor(MockExecutor):
    """Mock executor that fails for testing purposes."""

    def __init__(self):
        self.count = 0

    def execute(
        self,
        playbook: str,
        host: Optional[str] = None,
        extra_vars: Optional[Iterable[str]] = None,
    ):
        if self.count > 0:
            return OperationStateEnum.FAILURE, f"{playbook} LOG FAILURE".encode("utf-8")
        self.count += 1
        return super().execute(playbook, host, extra_vars)


@pytest.fixture
def mock_deployment_runner(
    mock_collections: Collections,
    mock_cluster_status: ClusterStatus,
    mock_cluster_variables: ClusterVariables,
):
    return DeploymentRunner(
        executor=MockExecutor(),
        collections=mock_collections,
        cluster_variables=mock_cluster_variables,
        cluster_status=mock_cluster_status,
    )


@pytest.fixture
def mock_deployment_runner_failing(
    mock_collections: Collections,
    mock_cluster_status: ClusterStatus,
    mock_cluster_variables: ClusterVariables,
):
    return DeploymentRunner(
        executor=FailingExecutor(),
        collections=mock_collections,
        cluster_variables=mock_cluster_variables,
        cluster_status=mock_cluster_status,
    )


# TODO: add asserts on cluster status


def test_deployment_plan_is_success(
    mock_dag: Dag, mock_deployment_runner: DeploymentRunner
):
    """Nominal case, runs a deployment with full DAG."""
    deployment_iterator = mock_deployment_runner.run(DeploymentModel.from_dag(mock_dag))

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment.operations) == 8
    for operation in deployment_iterator.deployment.operations:
        assert operation.state == OperationStateEnum.SUCCESS


def test_deployment_plan_with_filter_is_success(
    mock_dag: Dag, mock_deployment_runner: DeploymentRunner
):
    """Executing deployment from filtered dag should be a success."""
    deployment = DeploymentModel.from_dag(
        mock_dag, targets=["serv_init"], filter_expression="*_install"
    )
    deployment_iterator = mock_deployment_runner.run(deployment)

    for i, _ in enumerate(deployment_iterator):
        assert deployment.operations[i].state == OperationStateEnum.SUCCESS

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment.operations) == 2


def test_noop_deployment_plan_is_success(
    mock_collections: Collections, mock_deployment_runner: DeploymentRunner
):
    """Deployment plan containing only noop operation."""
    deployment = DeploymentModel.from_operations(mock_collections, ["serv_init"])
    deployment_iterator = mock_deployment_runner.run(deployment)

    for i, _ in enumerate(deployment_iterator):
        assert deployment.operations[i].state == OperationStateEnum.SUCCESS

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment.operations) == 1


def test_failed_operation_stops(
    mock_dag: Dag, mock_deployment_runner_failing: DeploymentRunner
):
    """Execution fails at the 2nd task."""
    deployment = DeploymentModel.from_dag(mock_dag, targets=["serv_init"])
    deployment_iterator = mock_deployment_runner_failing.run(deployment)

    for _ in deployment_iterator:
        pass
    assert deployment_iterator.deployment.status == DeploymentStateEnum.FAILURE
    assert len(deployment_iterator.deployment.operations) == 8


def test_service_log_is_emitted(
    mock_dag: Dag, mock_deployment_runner: DeploymentRunner
):
    """Executing 2 * config and restart (1 on component, 1 on service)."""
    deployment = DeploymentModel.from_dag(mock_dag, targets=["serv_init"])
    deployment_iterator = mock_deployment_runner.run(deployment)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS


def test_service_log_is_not_emitted(
    mock_dag: Dag, mock_deployment_runner: DeploymentRunner
):
    """Executing only install tasks, therefore no service log."""
    deployment = DeploymentModel.from_dag(
        mock_dag, targets=["serv_init"], filter_expression="*_install"
    )
    deployment_iterator = mock_deployment_runner.run(deployment)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS


def test_service_log_only_noop_is_emitted(
    mock_collections: Collections, mock_deployment_runner: DeploymentRunner
):
    """Deployment plan containing only noop config and start."""
    deployment = DeploymentModel.from_operations(
        mock_collections, ["serv_config", "serv_start"]
    )
    deployment_iterator = mock_deployment_runner.run(deployment)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS


def test_service_log_not_emitted_when_config_start_wrong_order(
    mock_collections: Collections, mock_deployment_runner: DeploymentRunner
):
    """Deployment plan containing start then config should not emit service log."""
    deployment = DeploymentModel.from_operations(
        mock_collections, ["serv_comp_start", "serv_comp_config"]
    )
    deployment_iterator = mock_deployment_runner.run(deployment)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS


def test_service_log_emitted_once_with_start_and_restart(
    mock_collections: Collections, mock_deployment_runner: DeploymentRunner
):
    """Deployment plan containing config, start, and restart should emit only one service log."""
    deployment = DeploymentModel.from_operations(
        mock_collections, ["serv_config", "serv_start", "serv_restart"]
    )
    deployment_iterator = mock_deployment_runner.run(deployment)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS


def test_service_log_emitted_once_with_multiple_config_and_start_on_same_component(
    mock_collections: Collections, mock_deployment_runner: DeploymentRunner
):
    """Deployment plan containing multiple config, start, and restart should emit only one service log."""
    deployment = DeploymentModel.from_operations(
        mock_collections,
        [
            "serv_comp_config",
            "serv_comp_start",
            "serv_comp_config",
            "serv_comp_restart",
        ],
    )
    deployment_iterator = mock_deployment_runner.run(deployment)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS


def test_deployment_dag_is_resumed(
    mock_dag: Dag,
    mock_deployment_runner_failing: DeploymentRunner,
    mock_deployment_runner: DeploymentRunner,
    mock_collections: Collections,
):
    deployment = DeploymentModel.from_dag(mock_dag, targets=["serv_init"])
    deployment_iterator = mock_deployment_runner_failing.run(deployment)
    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment.status == DeploymentStateEnum.FAILURE

    resume_log = DeploymentModel.from_failed_deployment(
        mock_collections, deployment_iterator.deployment
    )
    resume_deployment_iterator = mock_deployment_runner.run(resume_log)
    for _ in resume_deployment_iterator:
        pass

    assert (
        resume_deployment_iterator.deployment.deployment_type
        == DeploymentTypeEnum.RESUME
    )
    assert resume_deployment_iterator.deployment.status == DeploymentStateEnum.SUCCESS
    failed_operation = next(
        filter(
            lambda x: x.state == DeploymentStateEnum.FAILURE,
            deployment_iterator.deployment.operations,
        )
    )
    assert (
        failed_operation.operation
        == resume_deployment_iterator.deployment.operations[0].operation
    )
    assert len(
        deployment_iterator.deployment.operations
    ) - deployment_iterator.deployment.operations.index(failed_operation) == len(
        resume_deployment_iterator.deployment.operations
    )


@pytest.mark.skip(reason="from_reconfigure have been removed, to be reworked")
def test_deployment_reconfigure_is_resumed(
    mock_dag: Dag,
    reconfigurable_cluster_variables: ClusterVariables,
    mock_deployment_runner_failing: DeploymentRunner,
    mock_deployment_runner: DeploymentRunner,
    mock_collections: Collections,
):
    pass
    # (
    #     cluster_variables,
    #     component_version_deployed,
    # ) = reconfigurable_cluster_variables
    # deployment = DeploymentLog.from_reconfigure(
    #     dag, cluster_variables, component_version_deployed
    # )
    # deployment_iterator = failing_deployment_runner.run(deployment_log)
    # for _ in deployment_iterator:
    #     pass

    # assert deployment_iterator.deployment_log.status == DeploymentStateEnum.FAILURE
    # resume_log = DeploymentLog.from_failed_deployment(
    #     minimal_collections, deployment_iterator.deployment_log
    # )
    # resume_deployment_iterator = deployment_runner.run(resume_log)
    # for _ in resume_deployment_iterator:
    #     pass

    # assert (
    #     resume_deployment_iterator.deployment_log.deployment_type
    #     == DeploymentTypeEnum.RESUME
    # )
    # assert (
    #     resume_deployment_iterator.deployment_log.status == DeploymentStateEnum.SUCCESS
    # )
    # failed_operation = next(
    #     filter(
    #         lambda x: x.state == DeploymentStateEnum.FAILURE,
    #         deployment_iterator.deployment_log.operations,
    #     )
    # )
    # assert (
    #     failed_operation.operation
    #     == resume_deployment_iterator.deployment_log.operations[0].operation
    # )
    # assert len(
    #     deployment_iterator.deployment_log.operations
    # ) - deployment_iterator.deployment_log.operations.index(failed_operation) == len(
    #     resume_deployment_iterator.deployment_log.operations
    # )
