# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import random
import string
from typing import List, Optional

import pytest
from sqlalchemy.engine import Engine

from tdp.core.models.deployment_model import DeploymentModel
from tdp.core.models.enums import (
    DeploymentStateEnum,
    OperationStateEnum,
    SCHStatusLogSourceEnum,
)
from tdp.core.models.operation_model import OperationModel
from tdp.core.models.sch_status_log_model import SCHStatusLogModel
from tdp.dao import Dao
from tests.conftest import assert_equal_values_in_model, create_session

logger = logging.getLogger(__name__)


def _generate_version() -> str:
    """Generate a random version string."""
    return "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(6)
    )


def _set_seed(seed: Optional[str] = None) -> None:
    """Set the seed for random number generation and log the seed."""
    _seed = seed or _generate_version()  # Generate a random seed
    random.seed(_seed)
    logger.info(f"Random seed set to: {_seed}")


def _mock_sch_status_log(
    service: str,
    component: Optional[str],
    host: Optional[str],
    n: int = 50,
    seed: Optional[str] = None,
) -> List["SCHStatusLogModel"]:
    """Generate n mock SCHStatusLog entries."""
    _set_seed(seed)
    logs = []
    for _ in range(n):
        logs.append(
            SCHStatusLogModel(
                service=service,
                component=component,
                host=host,
                source=SCHStatusLogSourceEnum.STALE,
                running_version=(
                    _generate_version() if random.choice([True, False]) else None
                ),
                configured_version=(
                    _generate_version() if random.choice([True, False]) else None
                ),
                to_config=random.choice([True, False, None]),
                to_restart=random.choice([True, False, None]),
            )
        )
    return logs


def _last_values(
    logs: List["SCHStatusLogModel"],
):
    """Return an SCHStatusLog holding the last non None value for each column from a list of logs."""
    return (
        logs[-1].service,
        logs[-1].component,
        logs[-1].host,
        next(
            (
                log.running_version
                for log in reversed(logs)
                if log.running_version is not None
            ),
            None,
        ),
        next(
            (
                log.configured_version
                for log in reversed(logs)
                if log.configured_version is not None
            ),
            None,
        ),
        next(
            (log.to_config for log in reversed(logs) if log.to_config is not None), None
        ),
        next(
            (log.to_restart for log in reversed(logs) if log.to_restart is not None),
            None,
        ),
    )


@pytest.mark.skip(reason="db_session fixture needs to be reworked.")
@pytest.mark.parametrize("db_engine", [True], indirect=True)
def test_single_service_component_status(db_engine: Engine):
    """Test the get_sch_status query with a single sch."""
    logs = _mock_sch_status_log("smock", "cmock", "hmock", 5)
    last_values = _last_values(logs)

    # Use this instead of db_session.add_all() to ensure different timestamps
    with create_session(db_engine) as session:
        for log in logs:
            session.add(log)
            # Commit at each step to ensure different timestamps
            session.commit()

    with Dao(db_engine) as dao:
        assert dao.get_cluster_status() == [last_values]


@pytest.mark.skip(reason="db_session fixture needs to be reworked.")
@pytest.mark.parametrize("db_engine", [True], indirect=True)
def test_multiple_service_component_status(db_engine: Engine):
    """Test the get_sch_status query with multiple schs."""
    classic_component_logs = _mock_sch_status_log("smock", "cmock", "hmock")
    service_noop_logs = _mock_sch_status_log("smock", None, None)
    component_noop_logs = _mock_sch_status_log("smock", "cmock", None)
    service_logs = _mock_sch_status_log("smock", None, "hmock")

    log_lists = [
        classic_component_logs,
        service_noop_logs,
        component_noop_logs,
        service_logs,
    ]

    last_values = set([_last_values(log_list) for log_list in log_lists])

    # Create iterators for each log list to step through them.
    iterators = [iter(log_list) for log_list in log_lists]

    # Fetch the first log from each list. 'None' if the list is empty.
    next_logs = [next(it, None) for it in iterators]

    # Continue until all logs have been appended.
    while any(log is not None for log in next_logs):
        # Get indices of the lists that still have logs left.
        available_indices = [i for i, log in enumerate(next_logs) if log is not None]

        # Randomly select one of the available log lists.
        chosen_index = random.choice(available_indices)

        # Append the next log from the chosen list to the database.
        with create_session(db_engine) as session:
            session.add(next_logs[chosen_index])
            # Commit at each step to ensure different timestamps
            session.commit()

        # Update the next log for the chosen list. 'None' if no more logs are left.
        next_logs[chosen_index] = next(iterators[chosen_index], None)

    with Dao(db_engine) as dao:
        assert set(dao.get_cluster_status()) == last_values


@pytest.mark.parametrize("db_engine", [True], indirect=True)
def test_get_deployment(db_engine):
    with create_session(db_engine) as session:
        session.add(
            DeploymentModel(
                id=1,
                state=DeploymentStateEnum.RUNNING,
            )
        )
        session.commit()
    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            dao.get_deployment(1),
            DeploymentModel(
                id=1,
                state=DeploymentStateEnum.RUNNING,
            ),
        )


@pytest.mark.parametrize("db_engine", [True], indirect=True)
def test_get_planned_deployment(db_engine):
    with create_session(db_engine) as session:
        session.add(
            DeploymentModel(
                id=1,
                state=DeploymentStateEnum.PLANNED,
            )
        )
        session.commit()
    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            dao.get_planned_deployment(),
            DeploymentModel(
                id=1,
                state=DeploymentStateEnum.PLANNED,
            ),
        )


@pytest.mark.parametrize("db_engine", [True], indirect=True)
def test_get_last_deployment(db_engine):
    with create_session(db_engine) as session:
        session.add(
            DeploymentModel(
                id=2,
                state=DeploymentStateEnum.FAILURE,
            )
        )
        session.add(
            DeploymentModel(
                id=3,
                state=DeploymentStateEnum.SUCCESS,
            )
        )
        session.commit()
    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            dao.get_last_deployment(),
            DeploymentModel(
                id=3,
                state=DeploymentStateEnum.SUCCESS,
            ),
        )


@pytest.mark.parametrize("db_engine", [True], indirect=True)
def test_get_deployments(db_engine):
    with create_session(db_engine) as session:
        session.add(
            DeploymentModel(
                id=1,
                state=DeploymentStateEnum.SUCCESS,
            )
        )
        session.add(
            DeploymentModel(
                id=2,
                state=DeploymentStateEnum.PLANNED,
            )
        )
        session.commit()
    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            list(dao.get_last_deployments())[0],
            DeploymentModel(id=1, state=DeploymentStateEnum.SUCCESS),
        )
        assert assert_equal_values_in_model(
            list(dao.get_last_deployments())[1],
            DeploymentModel(id=2, state=DeploymentStateEnum.PLANNED),
        )


@pytest.mark.parametrize("db_engine", [True], indirect=True)
def test_operation(db_engine):
    with create_session(db_engine) as session:
        session.add(DeploymentModel(id=1, state=DeploymentStateEnum.SUCCESS))
        session.add(
            OperationModel(
                deployment_id=1,
                operation_order=1,
                operation="test_operation",
                state=OperationStateEnum.SUCCESS,
            )
        )
        session.commit()
    with Dao(db_engine) as dao:
        assert assert_equal_values_in_model(
            dao.get_operations_by_name(
                deployment_id=1, operation_name="test_operation"
            )[0],
            OperationModel(
                deployment_id=1,
                operation_order=1,
                operation="test_operation",
                state=OperationStateEnum.SUCCESS,
            ),
        )
