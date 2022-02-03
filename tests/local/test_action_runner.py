import logging
import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from tdp.core.dag import Dag
from tdp.core.models import Base
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.runner.action_runner import ActionRunner
from tdp.core.runner.executor import Executor, StateEnum

logger = logging.getLogger("tdp").getChild("test_action_runner")


class MockExecutor(Executor):
    def execute(self, action):
        return StateEnum.SUCCESS, f"{action} LOG SUCCESS".encode("utf-8")


@pytest.fixture(scope="session")
def session_class():
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True, future=True)
    Base.metadata.create_all(engine)
    session_class = sessionmaker(bind=engine)
    return session_class


@pytest.fixture(scope="function")
def action_runner():
    dag = Dag()
    executor = MockExecutor()
    return ActionRunner(dag, executor)


def test_run_actions(action_runner, session_class):
    deployment_log = action_runner.run_to_node("hdfs_init", node_filter="*_install")
    nb_actions = len(deployment_log.actions)
    with session_class() as session:
        session.add(deployment_log)
        session.commit()
        deployment = session.get(DeploymentLog, deployment_log.id)
        logger.info(deployment)
        logger.info(deployment.actions)
        assert nb_actions == len(deployment_log.actions)
