# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, TextIO

import pytest

from tdp.conftest import generate_collection_at_path
from tdp.core.collection import Collection
from tdp.core.collections import (
    Collections,
)
from tdp.core.inventory_reader import InventoryReader
from tdp.core.models.deployment_model import (
    DeploymentModel,
    DeploymentTypeEnum,
    NothingToResumeError,
)
from tdp.core.models.state_enum import DeploymentStateEnum, OperationStateEnum

if TYPE_CHECKING:
    from tdp.core.dag import Dag


class MockInventoryReader(InventoryReader):
    def __init__(self, hosts: list[str]):
        self.hosts = hosts

    def get_hosts(self, *args, **kwargs) -> list[str]:
        return self.hosts

    def get_hosts_from_playbook(self, fd: TextIO) -> set[str]:
        return set(self.hosts)


def fail_deployment(deployment: DeploymentModel, index_to_fail: int):
    deployment.state = DeploymentStateEnum.FAILURE
    for operation in deployment.operations:
        if operation.operation_order < index_to_fail:
            operation.state = OperationStateEnum.SUCCESS
        elif operation.operation_order == index_to_fail:
            operation.state = OperationStateEnum.FAILURE
        else:
            operation.state = OperationStateEnum.HELD
    return deployment


def set_success(deployment: DeploymentModel):
    deployment.state = DeploymentStateEnum.SUCCESS
    for operation in deployment.operations:
        operation.state = OperationStateEnum.SUCCESS


class TestFromOperations:
    def test_empty(self, mock_collections: Collections):
        operations_names = []
        deployment = DeploymentModel.from_operations(mock_collections, operations_names)

        assert len(deployment.operations) == 0
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {"operations": []}  # TODO should be {}
        assert deployment.state == DeploymentStateEnum.PLANNED

    def test_single_operation(self, mock_collections: Collections):
        operations_names = ["serv_comp_config"]
        deployment = DeploymentModel.from_operations(mock_collections, operations_names)

        assert [
            operation_rec.operation for operation_rec in deployment.operations
        ] == operations_names
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {"operations": operations_names}
        assert deployment.state == DeploymentStateEnum.PLANNED

    def test_single_restart_operation(self, mock_collections: Collections):
        operations_names = ["serv_comp_restart"]
        deployment = DeploymentModel.from_operations(mock_collections, operations_names)

        assert [
            operation_rec.operation for operation_rec in deployment.operations
        ] == operations_names
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {"operations": operations_names}
        assert deployment.state == DeploymentStateEnum.PLANNED

    def test_single_noop_opeation(self, mock_collections: Collections):
        operations_names = ["serv_config"]
        deployment = DeploymentModel.from_operations(mock_collections, operations_names)

        assert [
            operation_rec.operation for operation_rec in deployment.operations
        ] == operations_names
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {"operations": operations_names}
        assert deployment.state == DeploymentStateEnum.PLANNED

    def test_single_restart_noop_operation(self, mock_collections: Collections):
        operations_names = ["serv_restart"]
        deployment = DeploymentModel.from_operations(mock_collections, operations_names)

        assert [
            operation_rec.operation for operation_rec in deployment.operations
        ] == operations_names
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {"operations": operations_names}
        assert deployment.state == DeploymentStateEnum.PLANNED

    def test_multiple_operations(self, mock_collections: Collections):
        operations_names = ["serv_comp_config", "serv_comp_restart"]
        deployment = DeploymentModel.from_operations(mock_collections, operations_names)

        assert [
            operation_rec.operation for operation_rec in deployment.operations
        ] == operations_names
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {"operations": operations_names}
        assert deployment.state == DeploymentStateEnum.PLANNED

    def test_single_host(self, mock_collections: Collections):
        operations_names = ["serv_comp_config", "serv_comp_start"]
        host = "localhost"
        deployment = DeploymentModel.from_operations(
            mock_collections, operations_names, [host]
        )

        assert [
            operation_rec.operation for operation_rec in deployment.operations
        ] == operations_names
        for operation_rec in deployment.operations:
            assert operation_rec.host == host
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {
            "operations": operations_names,
            "hosts": [host],
        }
        assert deployment.state == DeploymentStateEnum.PLANNED

    def test_multiple_host(self, tmp_path_factory: pytest.TempPathFactory):
        operations_names = ["serv_comp_config", "serv_comp_start"]
        hosts = ["host2", "host3", "host1"]
        collection_path = tmp_path_factory.mktemp("multiple_host_collection")
        dag_service_operations = {
            "mock": [
                {"name": operations_names[0]},
                {"name": operations_names[1]},
            ]
        }
        generate_collection_at_path(collection_path, dag_service_operations, {})
        collection = Collection(collection_path, MockInventoryReader(hosts))
        collections = Collections.from_collection_list([collection])

        deployment = DeploymentModel.from_operations(
            collections, operations_names, hosts
        )

        assert deployment.operations[0].operation == operations_names[0]
        assert deployment.operations[0].host == hosts[0]
        assert deployment.operations[1].operation == operations_names[0]
        assert deployment.operations[1].host == hosts[1]
        assert deployment.operations[2].operation == operations_names[0]
        assert deployment.operations[2].host == hosts[2]

        assert deployment.operations[3].operation == operations_names[1]
        assert deployment.operations[3].host == hosts[0]
        assert deployment.operations[4].operation == operations_names[1]
        assert deployment.operations[4].host == hosts[1]
        assert deployment.operations[5].operation == operations_names[1]
        assert deployment.operations[5].host == hosts[2]
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {
            "operations": operations_names,
            "hosts": hosts,
        }
        assert deployment.state == DeploymentStateEnum.PLANNED

    def test_extra_vars(self, mock_collections: Collections):
        operations_names = ["serv_comp_config", "serv_comp_start"]
        extra_vars = ["foo1=bar1", "foo2=bar2"]
        deployment = DeploymentModel.from_operations(
            collections=mock_collections,
            operation_names=operations_names,
            extra_vars=extra_vars,
        )

        assert [
            operation_rec.operation for operation_rec in deployment.operations
        ] == operations_names
        for operation_rec in deployment.operations:
            assert operation_rec.extra_vars == extra_vars
        assert deployment.deployment_type == DeploymentTypeEnum.OPERATIONS
        assert deployment.options == {
            "operations": operations_names,
            "extra_vars": extra_vars,
        }
        assert deployment.state == DeploymentStateEnum.PLANNED


class TestFromDag:
    def test_deployment_plan_from_dag(self, mock_dag: Dag):
        deployment = DeploymentModel.from_dag(mock_dag, ["serv_start"])

        assert deployment.deployment_type == DeploymentTypeEnum.DAG
        assert len(deployment.operations) == 6
        assert any(
            filter(lambda op: op.operation == "serv_start", deployment.operations)
        )

    def test_deployment_plan_filter(self, mock_dag: Dag):
        deployment = DeploymentModel.from_dag(
            mock_dag, targets=["serv_init"], filter_expression="*_install"
        )

        assert all(
            filter(
                lambda operation: operation.operation.endswith("_install"),
                deployment.operations,
            )
        ), "Filter expression should have left only install operations from dag"

    def test_deployment_plan_restart(self, mock_dag: Dag):
        deployment = DeploymentModel.from_dag(
            mock_dag,
            targets=["serv_init"],
            restart=True,
        )

        assert any(
            filter(
                lambda operation: operation.operation.endswith("_restart"),
                deployment.operations,
            )
        ), "A restart operation should be present"
        assert not any(
            filter(
                lambda operation: "_start" in operation.operation,
                deployment.operations,
            )
        ), "The restart flag should have removed every start operations from dag"


class TestFromFailedDeployment:
    def test_deployment_plan_resume_from_dag(
        self, mock_dag: Dag, mock_collections: Collections
    ):
        deployment = DeploymentModel.from_dag(
            mock_dag,
            targets=["serv_init"],
        )
        index_to_fail = 2
        deployment = fail_deployment(deployment, index_to_fail)

        resume_deployment = DeploymentModel.from_failed_deployment(
            mock_collections, deployment
        )
        assert resume_deployment.deployment_type == DeploymentTypeEnum.RESUME
        # index starts at 1
        assert len(deployment.operations) - index_to_fail + 1 == len(
            resume_deployment.operations
        )
        assert len(deployment.operations) >= len(resume_deployment.operations)

    def test_deployment_plan_resume_with_success_deployment(
        self, mock_dag: Dag, mock_collections: Collections
    ):
        deployment = DeploymentModel.from_dag(
            mock_dag,
            targets=["serv_init"],
        )
        set_success(deployment)
        with pytest.raises(NothingToResumeError):
            DeploymentModel.from_failed_deployment(mock_collections, deployment)

    def test_deployment_plan_resume_from_operations_host(
        self, mock_collections: Collections
    ):
        operations_names = ["serv_comp_install", "serv_comp_config", "serv_comp_start"]
        host = "localhost"
        deployment = DeploymentModel.from_operations(
            mock_collections,
            operation_names=operations_names,
            host_names=[host],
        )
        index_to_fail = 2
        deployment = fail_deployment(deployment, index_to_fail)

        resume_deployment = DeploymentModel.from_failed_deployment(
            mock_collections, deployment
        )
        assert resume_deployment.deployment_type == DeploymentTypeEnum.RESUME
        # index starts at 1
        assert len(deployment.operations) - index_to_fail + 1 == len(
            resume_deployment.operations
        )
        assert len(deployment.operations) >= len(resume_deployment.operations)
        for operation_rec in resume_deployment.operations:
            assert operation_rec.host == host

    def test_deployment_plan_resume_from_operations_extra_vars(
        self, mock_collections: Collections
    ):
        operations_names = ["serv_comp_install", "serv_comp_config", "serv_comp_start"]
        extra_vars = ["foo1=bar1", "foo2=bar2"]
        deployment = DeploymentModel.from_operations(
            mock_collections,
            operation_names=operations_names,
            extra_vars=extra_vars,
        )
        index_to_fail = 2
        deployment = fail_deployment(deployment, index_to_fail)

        resume_deployment = DeploymentModel.from_failed_deployment(
            mock_collections, deployment
        )
        assert resume_deployment.deployment_type == DeploymentTypeEnum.RESUME
        # index starts at 1
        assert len(deployment.operations) - index_to_fail + 1 == len(
            resume_deployment.operations
        )
        assert len(deployment.operations) >= len(resume_deployment.operations)
        for operation_rec in resume_deployment.operations:
            assert operation_rec.extra_vars == extra_vars


@pytest.mark.skip(reason="test to rewrite using cluster_status")
class TestFromStaleComponents:
    pass
    # def test_nothing_stale(self, mock_collections: Collections):
    #     stale_components = []
    #     with pytest.raises(NothingToReconfigureError):
    #         DeploymentLog.from_stale_components(
    #             collections=mock_collections,
    #             stale_components=stale_components,
    #         )

    # def test_one_stale(self, mock_collections: Collections):
    #     stale_components = [
    #         StaleComponent(
    #             service="mock",
    #             component="node",
    #             to_reconfigure=True,
    #             to_restart=True,
    #         )
    #     ]
    #     deployment_log = DeploymentLog.from_stale_components(
    #         collections=mock_collections,
    #         stale_components=stale_components,
    #     )

    #     assert len(deployment_log.operations) == 2
    #     assert deployment_log.deployment_type == DeploymentTypeEnum.RECONFIGURE

    # def test_one_stale_rolling(self, tmp_path_factory: pytest.TempPathFactory):
    #     operations_names = ["serv_comp_config", "serv_comp_start"]
    #     operation_name_restart = "serv_comp_restart"
    #     hosts = ["host2", "host3", "host1"]
    #     sorted_hosts = sorted(hosts)
    #     rolling_interval = 1
    #     collection_path = tmp_path_factory.mktemp("multiple_host_rolling_collection")
    #     dag_service_operations = {
    #         "mock": [
    #             {"name": operations_names[0]},
    #             {"name": operations_names[1]},
    #         ]
    #     }
    #     generate_collection_at_path(collection_path, dag_service_operations, {})
    #     collection = Collection.from_path(collection_path)
    #     collection._inventory_reader = MockInventoryReader(hosts)
    #     collections = Collections.from_collection_list([collection])

    #     stale_components = [
    #         StaleComponent(
    #             service="mock",
    #             component="node",
    #             host=host,
    #             to_reconfigure=True,
    #             to_restart=True,
    #         )
    #         for host in hosts
    #     ]

    #     deployment_log = DeploymentLog.from_stale_components(
    #         collections=collections,
    #         stale_components=stale_components,
    #         rolling_interval=rolling_interval,
    #     )

    #     assert len(deployment_log.operations) == 9
    #     assert deployment_log.deployment_type == DeploymentTypeEnum.RECONFIGURE
    #     assert deployment_log.options == {
    #         "rolling_interval": rolling_interval,
    #     }

    #     # Config operations
    #     assert deployment_log.operations[0].operation == operations_names[0]
    #     assert deployment_log.operations[0].host == sorted_hosts[0]
    #     assert deployment_log.operations[0].extra_vars == None

    #     assert deployment_log.operations[1].operation == operations_names[0]
    #     assert deployment_log.operations[1].host == sorted_hosts[1]
    #     assert deployment_log.operations[1].extra_vars == None

    #     assert deployment_log.operations[2].operation == operations_names[0]
    #     assert deployment_log.operations[2].host == sorted_hosts[2]
    #     assert deployment_log.operations[2].extra_vars == None

    #     # Restart and sleep operations
    #     assert deployment_log.operations[3].operation == operation_name_restart
    #     assert deployment_log.operations[3].host == sorted_hosts[0]
    #     assert deployment_log.operations[3].extra_vars == None

    #     assert deployment_log.operations[4].operation == OPERATION_SLEEP_NAME
    #     assert deployment_log.operations[4].host == None
    #     assert deployment_log.operations[4].extra_vars == [
    #         f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"
    #     ]

    #     assert deployment_log.operations[5].operation == operation_name_restart
    #     assert deployment_log.operations[5].host == sorted_hosts[1]
    #     assert deployment_log.operations[5].extra_vars == None

    #     assert deployment_log.operations[6].operation == OPERATION_SLEEP_NAME
    #     assert deployment_log.operations[6].host == None
    #     assert deployment_log.operations[6].extra_vars == [
    #         f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"
    #     ]

    #     assert deployment_log.operations[7].operation == operation_name_restart
    #     assert deployment_log.operations[7].host == sorted_hosts[2]
    #     assert deployment_log.operations[7].extra_vars == None

    #     assert deployment_log.operations[8].operation == OPERATION_SLEEP_NAME
    #     assert deployment_log.operations[8].host == None
    #     assert deployment_log.operations[8].extra_vars == [
    #         f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"
    #     ]
