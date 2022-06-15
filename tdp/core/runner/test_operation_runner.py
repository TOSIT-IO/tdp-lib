# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging

import pytest

from tdp.core.collection import YML_EXTENSION, Collection
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
        class FakeDict:
            def __getitem__(self, value):
                return str(value) + YML_EXTENSION

        return FakeDict()


@pytest.fixture(scope="function")
def mock_dag():
    dag = object.__new__(Dag)
    dag._collections = {"pytest": MockCollection("/pytest")}
    dag.operations = {
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
    return dag


@pytest.fixture(scope="function")
def operation_runner(mock_dag):
    executor = MockExecutor()
    service_manager = MockServiceManager("zookeeper", None, mock_dag)
    service_managers = {
        "zookeeper": service_manager,
    }
    return OperationRunner(mock_dag, executor, service_managers)


def test_operation_runner(operation_runner):
    """Mock tests"""
    deployment_log = operation_runner.run_nodes(
        targets=["zookeeper_init"], node_filter="*_install"
    )
    logger.info(deployment_log)
    logger.info(deployment_log.operations)
    assert not any(
        filter(
            lambda operation_log: "_init" in operation_log.operation,
            deployment_log.operations,
        )
    ), "Filter should have removed every init operations from dag"
