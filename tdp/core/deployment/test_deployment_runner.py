# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import pytest

from tdp.core.dag import Dag
from tdp.core.models import (
    DeploymentLog,
    DeploymentStateEnum,
    DeploymentTypeEnum,
    OperationStateEnum,
)

from .deployment_runner import DeploymentRunner
from .executor import Executor


class MockExecutor(Executor):
    def execute(self, playbook, host=None):
        return OperationStateEnum.SUCCESS, f"{playbook} LOG SUCCESS".encode("utf-8")


class FailingExecutor(MockExecutor):
    def __init__(self):
        self.count = 0

    def execute(self, playbook, host=None):
        if self.count > 0:
            return OperationStateEnum.FAILURE, f"{playbook} LOG FAILURE".encode("utf-8")
        self.count += 1
        return super().execute(playbook, host)


@pytest.fixture
def deployment_runner(minimal_collections, cluster_variables):
    return DeploymentRunner(
        minimal_collections, MockExecutor(), cluster_variables, stale_components=[]
    )


@pytest.fixture
def failing_deployment_runner(minimal_collections, cluster_variables):
    return DeploymentRunner(
        minimal_collections, FailingExecutor(), cluster_variables, stale_components=[]
    )


def test_deployment_plan_is_success(dag: Dag, deployment_runner: DeploymentRunner):
    """nominal case, running a deployment with full dag"""
    deployment_log = DeploymentLog.from_dag(dag)
    deployment_iterator = deployment_runner.run(deployment_log)

    for operation, _, _ in deployment_iterator:
        if operation is not None:
            assert operation.state == OperationStateEnum.SUCCESS

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.operations) == 8
    assert len(deployment_iterator.deployment_log.component_version) == 2


def test_deployment_plan_with_filter_is_success(
    dag: Dag, deployment_runner: DeploymentRunner
):
    """executing deployment from filtered dag should be a success"""
    deployment_log = DeploymentLog.from_dag(
        dag, targets=["mock_init"], filter_expression="*_install"
    )
    deployment_iterator = deployment_runner.run(deployment_log)

    for operation_log, _, _ in deployment_iterator:
        if operation_log is not None:
            assert operation_log.state == OperationStateEnum.SUCCESS

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.operations) == 2


def test_noop_deployment_plan_is_success(minimal_collections, deployment_runner):
    """deployment plan containing only noop operation"""
    deployment_log = DeploymentLog.from_operations(minimal_collections, ["mock_init"])
    deployment_iterator = deployment_runner.run(deployment_log)

    for operation, _, _ in deployment_iterator:
        if operation is not None:
            assert operation.state == OperationStateEnum.SUCCESS

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.operations) == 1


def test_failed_operation_stops(dag, failing_deployment_runner):
    """execution fails at the 2 task"""
    deployment_log = DeploymentLog.from_dag(dag, targets=["mock_init"])
    deployment_iterator = failing_deployment_runner.run(deployment_log)

    for _ in deployment_iterator:
        pass
    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.FAILURE
    assert len(deployment_iterator.deployment_log.operations) == 8


def test_service_log_is_emitted(dag, deployment_runner):
    """executing 2 * config and restart (1 on component, 1 on service)"""
    deployment_log = DeploymentLog.from_dag(dag, targets=["mock_init"])
    deployment_iterator = deployment_runner.run(deployment_log)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.component_version) == 2


def test_service_log_is_not_emitted(dag, deployment_runner):
    """executing only install tasks, therefore no service log"""
    deployment_log = DeploymentLog.from_dag(
        dag, targets=["mock_init"], filter_expression="*_install"
    )
    deployment_iterator = deployment_runner.run(deployment_log)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.component_version) == 0


def test_service_log_only_noop_is_emitted(minimal_collections, deployment_runner):
    """deployment plan containing only noop config and start"""
    deployment_log = DeploymentLog.from_operations(
        minimal_collections, ["mock_config", "mock_start"]
    )
    deployment_iterator = deployment_runner.run(deployment_log)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.component_version) == 1


def test_service_log_not_emitted_when_config_start_wrong_order(
    minimal_collections, deployment_runner
):
    """deployment plan containing start then config should not emit service log"""
    deployment_log = DeploymentLog.from_operations(
        minimal_collections, ["mock_node_start", "mock_node_config"]
    )
    deployment_iterator = deployment_runner.run(deployment_log)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.component_version) == 0


def test_service_log_emitted_once_with_start_and_restart(
    minimal_collections, deployment_runner
):
    """deployment plan containing config, start, and restart should emit only one service log"""
    deployment_log = DeploymentLog.from_operations(
        minimal_collections, ["mock_config", "mock_start", "mock_restart"]
    )
    deployment_iterator = deployment_runner.run(deployment_log)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.component_version) == 1


def test_service_log_emitted_once_with_multiple_config_and_start_on_same_component(
    minimal_collections, deployment_runner
):
    """deployment plan containing multiple config, start, and restart should emit only one service log"""
    deployment_log = DeploymentLog.from_operations(
        minimal_collections,
        [
            "mock_node_config",
            "mock_node_start",
            "mock_node_config",
            "mock_node_restart",
        ],
    )
    deployment_iterator = deployment_runner.run(deployment_log)

    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    assert len(deployment_iterator.deployment_log.component_version) == 1


def test_deployment_dag_is_resumed(
    dag, failing_deployment_runner, deployment_runner, minimal_collections
):
    deployment_log = DeploymentLog.from_dag(dag, targets=["mock_init"])
    deployment_iterator = failing_deployment_runner.run(deployment_log)
    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.FAILURE

    resume_log = DeploymentLog.from_failed_deployment(
        minimal_collections, deployment_iterator.deployment_log
    )
    resume_deployment_iterator = deployment_runner.run(resume_log)
    for _ in resume_deployment_iterator:
        pass

    assert (
        resume_deployment_iterator.deployment_log.deployment_type
        == DeploymentTypeEnum.RESUME
    )
    assert (
        resume_deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    )
    failed_operation = next(
        filter(
            lambda x: x.state == DeploymentStateEnum.FAILURE,
            deployment_iterator.deployment_log.operations,
        )
    )
    assert (
        failed_operation.operation
        == resume_deployment_iterator.deployment_log.operations[0].operation
    )
    assert len(
        deployment_iterator.deployment_log.operations
    ) - deployment_iterator.deployment_log.operations.index(failed_operation) == len(
        resume_deployment_iterator.deployment_log.operations
    )


@pytest.mark.skip(reason="from_reconfigure have been removed, to be reworked")
def test_deployment_reconfigure_is_resumed(
    dag,
    reconfigurable_cluster_variables,
    failing_deployment_runner,
    deployment_runner,
    minimal_collections,
):
    (
        cluster_variables,
        component_version_deployed,
    ) = reconfigurable_cluster_variables
    deployment_log = DeploymentLog.from_reconfigure(
        dag, cluster_variables, component_version_deployed
    )
    deployment_iterator = failing_deployment_runner.run(deployment_log)
    for _ in deployment_iterator:
        pass

    assert deployment_iterator.deployment_log.state == DeploymentStateEnum.FAILURE
    resume_log = DeploymentLog.from_failed_deployment(
        minimal_collections, deployment_iterator.deployment_log
    )
    resume_deployment_iterator = deployment_runner.run(resume_log)
    for _ in resume_deployment_iterator:
        pass

    assert (
        resume_deployment_iterator.deployment_log.deployment_type
        == DeploymentTypeEnum.RESUME
    )
    assert (
        resume_deployment_iterator.deployment_log.state == DeploymentStateEnum.SUCCESS
    )
    failed_operation = next(
        filter(
            lambda x: x.state == DeploymentStateEnum.FAILURE,
            deployment_iterator.deployment_log.operations,
        )
    )
    assert (
        failed_operation.operation
        == resume_deployment_iterator.deployment_log.operations[0].operation
    )
    assert len(
        deployment_iterator.deployment_log.operations
    ) - deployment_iterator.deployment_log.operations.index(failed_operation) == len(
        resume_deployment_iterator.deployment_log.operations
    )
