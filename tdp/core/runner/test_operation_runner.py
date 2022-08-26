# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import OrderedDict

import pytest

from tdp.core.collection import YML_EXTENSION, Collection
from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.operation import Operation
from tdp.core.runner.executor import Executor, StateEnum
from tdp.core.runner.operation_runner import OperationRunner
from tdp.core.service_manager import ServiceManager

logger = logging.getLogger("tdp").getChild("test_operation_runner")


class MockExecutor(Executor):
    def execute(self, operation):
        return StateEnum.SUCCESS, f"{operation} LOG SUCCESS".encode("utf-8")


class MockServiceManager(ServiceManager):
    def version(self):
        return "foo"


class MockCollection(Collection):
    @property
    def operations(self):
        return {
            "zookeeper_server_install": "zookeeper_server_install" + YML_EXTENSION,
            "zookeeper_server_config": "zookeeper_server_config" + YML_EXTENSION,
            "zookeeper_server_start": "zookeeper_server_start" + YML_EXTENSION,
            "zookeeper_server_restart": "zookeeper_server_restart" + YML_EXTENSION,
            "zookeeper_server_init": "zookeeper_server_init" + YML_EXTENSION,
            "zookeeper_init": "zookeeper_init" + YML_EXTENSION,
        }


class MockCollections(Collections):
    def _init_operations(self):
        pass


@pytest.fixture(scope="function")
def mock_dag():
    collections_dict = OrderedDict(
        [
            ("pytest", MockCollection("/pytest")),
        ]
    )
    collections = MockCollections(collections_dict)
    collections._dag_operations = {
        "zookeeper_server_install": Operation("zookeeper_server_install", "pytest"),
        "zookeeper_server_config": Operation(
            "zookeeper_server_config", "pytest", depends_on=["zookeeper_server_install"]
        ),
        "zookeeper_server_start": Operation(
            "zookeeper_server_start", "pytest", depends_on=["zookeeper_server_config"]
        ),
        "zookeeper_server_init": Operation(
            "zookeeper_server_init", "pytest", depends_on=["zookeeper_server_start"]
        ),
        "zookeeper_init": Operation(
            "zookeeper_init", "pytest", depends_on=["zookeeper_server_init"]
        ),
    }
    collections._other_operations = {
        "zookeeper_server_restart": Operation(
            "zookeeper_server_restart",
            "pytest",
        ),
    }
    dag = Dag(collections)
    return dag


@pytest.fixture(scope="function")
def operation_runner(mock_dag):
    executor = MockExecutor()
    service_manager = MockServiceManager("zookeeper", None, mock_dag)
    service_managers = {
        "zookeeper": service_manager,
    }
    return OperationRunner(mock_dag, executor, service_managers)


def test_operation_runner_filter(operation_runner):
    operation_iterator = operation_runner.run_nodes(
        targets=["zookeeper_init"], filter_expression="*_install"
    )

    list(operation_iterator)
    deployment_log = operation_iterator.deployment_log
    logger.info(deployment_log)
    logger.info(deployment_log.operations)
    assert not any(
        filter(
            lambda operation_log: "_init" in operation_log.operation,
            deployment_log.operations,
        )
    ), "Filter should have removed every init operations from dag"


def test_operation_runner_restart(operation_runner):
    operation_iterator = operation_runner.run_nodes(
        targets=["zookeeper_init"],
        restart=True,
    )
    list(operation_iterator)
    deployment_log = operation_iterator.deployment_log
    logger.info(deployment_log)
    logger.info(deployment_log.operations)
    assert any(
        filter(
            lambda operation_log: "_restart" in operation_log.operation,
            deployment_log.operations,
        )
    ), "A restart operation should be present"
    assert not any(
        filter(
            lambda operation_log: "_start" in operation_log.operation,
            deployment_log.operations,
        )
    ), "The restart flag should have removed every start operations from dag"
