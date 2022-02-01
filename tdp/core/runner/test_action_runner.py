import logging
import pytest


from tdp.core.dag import Dag
from tdp.core.runner.action_runner import ActionRunner
from tdp.core.runner.executor import Executor, StateEnum

logger = logging.getLogger("tdp").getChild("test_action_runner")


class MockExecutor(Executor):
    def execute(self, action):
        return StateEnum.SUCCESS, f"{action} LOG SUCCESS".encode("utf-8")


@pytest.fixture(scope="function")
def action_runner():
    dag = Dag()
    executor = MockExecutor()
    return ActionRunner(dag, executor)


def test_action_runner(action_runner):
    """Mock tests"""
    deployment_log = action_runner.run_to_node("hdfs_init", node_filter="*_install")
    logger.info(deployment_log)
    logger.info(deployment_log.actions)
    assert not any(
        filter(lambda action_log: "_init" in action_log.action, deployment_log.actions)
    ), "Filter should have removed every init actions from dag"
