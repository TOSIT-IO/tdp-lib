# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging

import pytest

from tdp.core.collection import YML_EXTENSION, Collection
from tdp.core.component import Component
from tdp.core.dag import Dag
from tdp.core.runner.action_runner import ActionRunner
from tdp.core.runner.executor import Executor, StateEnum
from tdp.core.service_manager import ServiceManager

logger = logging.getLogger("tdp").getChild("test_action_runner")


class MockExecutor(Executor):
    def execute(self, action):
        return StateEnum.SUCCESS, f"{action} LOG SUCCESS".encode("utf-8")


class MockServiceManager(ServiceManager):
    def version(self):
        return "foo"


class MockCollection(Collection):
    @property
    def actions(self):
        class FakeDict:
            def __getitem__(self, value):
                return str(value) + YML_EXTENSION

        return FakeDict()


@pytest.fixture(scope="function")
def mock_dag():
    dag = object.__new__(Dag)
    dag._collections = {"pytest": MockCollection("/pytest")}
    dag.components = {
        "zookeeper_server_install": Component("zookeeper_server_install", "pytest"),
        "zookeeper_server_config": Component(
            "zookeeper_server_config", "pytest", depends_on=["zookeeper_server_install"]
        ),
        "zookeeper_server_start": Component(
            "zookeeper_server_start", "pytest", depends_on=["zookeeper_server_config"]
        ),
        "zookeeper_server_init": Component(
            "zookeeper_server_init", "pytest", depends_on=["zookeeper_server_start"]
        ),
        "zookeeper_init": Component(
            "zookeeper_init", "pytest", depends_on=["zookeeper_server_init"]
        ),
    }
    return dag


@pytest.fixture(scope="function")
def action_runner(mock_dag):
    executor = MockExecutor()
    service_manager = MockServiceManager("zookeeper", None, mock_dag)
    service_managers = {
        "zookeeper": service_manager,
    }
    return ActionRunner(mock_dag, executor, service_managers)


def test_action_runner(action_runner):
    """Mock tests"""
    deployment_log = action_runner.run_nodes(
        targets=["zookeeper_init"], node_filter="*_install"
    )
    logger.info(deployment_log)
    logger.info(deployment_log.actions)
    assert not any(
        filter(lambda action_log: "_init" in action_log.action, deployment_log.actions)
    ), "Filter should have removed every init actions from dag"
