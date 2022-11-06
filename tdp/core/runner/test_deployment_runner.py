# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging

import pytest

from tdp.core.models import StateEnum

from .deployment_plan import DeploymentPlan
from .deployment_runner import DeploymentRunner
from .executor import Executor

logger = logging.getLogger("tdp").getChild("test_deployment_runner")


class MockExecutor(Executor):
    def execute(self, operation):
        return StateEnum.SUCCESS, f"{operation} LOG SUCCESS".encode("utf-8")


@pytest.fixture
def deployment_runner(minimal_collections, cluster_variables):
    return DeploymentRunner(minimal_collections, MockExecutor(), cluster_variables)


def test_deployment_plan_is_success(dag, deployment_runner):
    deployment_plan = DeploymentPlan.from_dag(
        dag, targets=["mock_init"], filter_expression="*_install"
    )
    deployment_iterator = deployment_runner.run(deployment_plan)

    for operation in deployment_iterator:
        assert operation.state == StateEnum.SUCCESS

    assert deployment_iterator.log.state == StateEnum.SUCCESS
    assert len(deployment_iterator.log.operations) == 1
    assert len(deployment_iterator.log.services) > 0


def test_noop_deployment_plan_is_success(minimal_collections, deployment_runner):
    deployment_plan = DeploymentPlan.from_operations(
        [minimal_collections.operations["mock_init"]]
    )
    deployment_iterator = deployment_runner.run(deployment_plan)

    for operation in deployment_iterator:
        assert operation.state == StateEnum.SUCCESS

    assert deployment_iterator.log.state == StateEnum.SUCCESS
    assert len(deployment_iterator.log.operations) == 0
    assert len(deployment_iterator.log.services) > 0
