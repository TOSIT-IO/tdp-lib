# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.variables import ClusterVariables

from .component_version_log import ComponentVersionLog
from .deployment_log import (
    DeploymentLog,
    DeploymentTypeEnum,
    NothingToReconfigureError,
    NothingToResumeError,
)
from .operation_log import OperationLog
from .state_enum import DeploymentStateEnum, OperationStateEnum


def fail_deployment_log(deployment_log: DeploymentLog, index_to_fail: int):
    deployment_log.state = DeploymentStateEnum.FAILURE
    for operation in deployment_log.operations:
        if operation.operation_order < index_to_fail:
            operation.state = OperationStateEnum.SUCCESS
        elif operation.operation_order == index_to_fail:
            operation.state = OperationStateEnum.FAILURE
        else:
            operation.state = OperationStateEnum.HELD
    return deployment_log


def set_success(deployment_log):
    deployment_log.state = DeploymentStateEnum.SUCCESS
    for operation in deployment_log.operations:
        operation.state = OperationStateEnum.SUCCESS


class TestFromOperations:
    def test_empty(self, minimal_collections: Collections):
        operations_names = []
        deployment_log = DeploymentLog.from_operations(
            minimal_collections, operations_names
        )

        assert len(deployment_log.operations) == 0
        assert deployment_log.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment_log.targets == []
        assert deployment_log.sources == None
        assert deployment_log.filter_expression == None
        assert deployment_log.filter_type == None
        assert deployment_log.restart == None

    def test_single_operation(self, minimal_collections: Collections):
        operations_names = ["mock_node_config"]
        deployment_log = DeploymentLog.from_operations(
            minimal_collections, operations_names
        )

        assert [
            operation_log.operation for operation_log in deployment_log.operations
        ] == operations_names
        assert deployment_log.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment_log.targets == operations_names
        assert deployment_log.sources == None
        assert deployment_log.filter_expression == None
        assert deployment_log.filter_type == None
        assert deployment_log.restart == None

    def test_single_restart_operation(self, minimal_collections: Collections):
        operations_names = ["mock_node_restart"]
        deployment_log = DeploymentLog.from_operations(
            minimal_collections, operations_names
        )

        assert [
            operation_log.operation for operation_log in deployment_log.operations
        ] == operations_names
        assert deployment_log.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment_log.targets == operations_names
        assert deployment_log.sources == None
        assert deployment_log.filter_expression == None
        assert deployment_log.filter_type == None
        assert deployment_log.restart == None  # restart flag is only used in dag

    def test_single_noop_opeation(self, minimal_collections: Collections):
        operations_names = ["mock_config"]
        deployment_log = DeploymentLog.from_operations(
            minimal_collections, operations_names
        )

        assert [
            operation_log.operation for operation_log in deployment_log.operations
        ] == operations_names
        assert deployment_log.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment_log.targets == operations_names
        assert deployment_log.sources == None
        assert deployment_log.filter_expression == None
        assert deployment_log.filter_type == None
        assert deployment_log.restart == None  # restart flag is only used in dag

    def test_single_restart_noop_operation(self, minimal_collections: Collections):
        operations_names = ["mock_restart"]
        deployment_log = DeploymentLog.from_operations(
            minimal_collections, operations_names
        )

        assert [
            operation_log.operation for operation_log in deployment_log.operations
        ] == operations_names
        assert deployment_log.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment_log.targets == operations_names
        assert deployment_log.sources == None
        assert deployment_log.filter_expression == None
        assert deployment_log.filter_type == None
        assert deployment_log.restart == None  # restart flag is only used in dag

    def test_multiple_operations(self, minimal_collections: Collections):
        operations_names = ["mock_node_config", "mock_node_restart"]
        deployment_log = DeploymentLog.from_operations(
            minimal_collections, operations_names
        )

        assert [
            operation_log.operation for operation_log in deployment_log.operations
        ] == operations_names
        assert deployment_log.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment_log.targets == operations_names
        assert deployment_log.sources == None
        assert deployment_log.filter_expression == None
        assert deployment_log.filter_type == None
        assert deployment_log.restart == None  # restart flag is only used in dag


class TestFromDag:
    def test_deployment_plan_from_dag(self, dag: Dag):
        deployment_log = DeploymentLog.from_dag(dag, ["mock_start"])

        assert deployment_log.deployment_type == DeploymentTypeEnum.DAG
        assert len(deployment_log.operations) == 6
        assert any(
            filter(lambda op: op.operation == "mock_start", deployment_log.operations)
        )

    def test_deployment_plan_filter(self, dag: Dag):
        deployment_log = DeploymentLog.from_dag(
            dag, targets=["mock_init"], filter_expression="*_install"
        )

        assert all(
            filter(
                lambda operation: operation.operation.endswith("_install"),
                deployment_log.operations,
            )
        ), "Filter expression should have left only install operations from dag"

    def test_deployment_plan_restart(self, dag: Dag):
        deployment_log = DeploymentLog.from_dag(
            dag,
            targets=["mock_init"],
            restart=True,
        )

        assert any(
            filter(
                lambda operation: operation.operation.endswith("_restart"),
                deployment_log.operations,
            )
        ), "A restart operation should be present"
        assert not any(
            filter(
                lambda operation: "_start" in operation.operation,
                deployment_log.operations,
            )
        ), "The restart flag should have removed every start operations from dag"


class TestFromFailedDeployment:
    def test_deployment_plan_resume_from_dag(
        self, dag: Dag, minimal_collections: Collections
    ):
        deployment_log = DeploymentLog.from_dag(
            dag,
            targets=["mock_init"],
        )
        index_to_fail = 2
        deployment_log = fail_deployment_log(deployment_log, index_to_fail)

        resume_deployment_log = DeploymentLog.from_failed_deployment(
            minimal_collections, deployment_log
        )
        assert resume_deployment_log.deployment_type == DeploymentTypeEnum.RESUME
        # index starts at 1
        assert len(deployment_log.operations) - index_to_fail + 1 == len(
            resume_deployment_log.operations
        )
        assert len(deployment_log.operations) >= len(resume_deployment_log.operations)

    def test_deployment_plan_resume_with_success_deployment(
        self, dag: Dag, minimal_collections: Collections
    ):
        deployment_log = DeploymentLog.from_dag(
            dag,
            targets=["mock_init"],
        )
        set_success(deployment_log)
        with pytest.raises(NothingToResumeError):
            DeploymentLog.from_failed_deployment(minimal_collections, deployment_log)


class TestFromStaleComponents:
    def test_deployment_plan_reconfigure_nothing_to_restart(
        self, dag: Dag, cluster_variables: ClusterVariables
    ):
        component_version_log = ComponentVersionLog(
            service="mock",
            component="node",
            version=cluster_variables["mock"].version,
        )
        with pytest.raises(NothingToReconfigureError):
            DeploymentLog.from_reconfigure(
                dag,
                cluster_variables,
                [component_version_log],
            )

    def test_deployment_plan_reconfigure(
        self, dag: Dag, reconfigurable_cluster_variables: ClusterVariables
    ):
        (
            cluster_variables,
            component_version_deployed,
        ) = reconfigurable_cluster_variables
        deployment_log = DeploymentLog.from_reconfigure(
            dag, cluster_variables, component_version_deployed
        )

        assert len(deployment_log.operations) == 4
        assert deployment_log.deployment_type == DeploymentTypeEnum.RECONFIGURE
