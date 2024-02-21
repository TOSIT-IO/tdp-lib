# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import random
import string
from typing import List, Optional, Union

from sqlalchemy.orm import Session

from tdp.cli.queries import get_sch_status
from tdp.core.models.sch_status_log_model import (
    SCHStatusLogModel,
    SCHStatusLogSourceEnum,
)

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
) -> tuple[
    str,
    Union[str, None],
    Union[str, None],
    Union[str, None],
    Union[str, None],
    Union[bool, None],
    Union[bool, None],
]:
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


def test_single_service_component_status(db_session: Session):
    """Test the get_sch_status query with a single sch."""
    logs = _mock_sch_status_log("smock", "cmock", "hmock", 5)
    last_values = _last_values(logs)

    # Use this instead of db_session.add_all() to ensure different timestamps
    for log in logs:
        db_session.add(log)
        db_session.commit()

    session_values = get_sch_status(db_session)[0]
    assert (
        session_values.service,
        session_values.component,
        session_values.host,
        session_values.running_version,
        session_values.configured_version,
        session_values.to_config,
        session_values.to_restart,
    ) == last_values


def test_multiple_service_component_status(db_session: Session):
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
        db_session.add(next_logs[chosen_index])
        db_session.commit()

        # Update the next log for the chosen list. 'None' if no more logs are left.
        next_logs[chosen_index] = next(iterators[chosen_index], None)

        session_rows = [
            (
                row.service,
                row.component,
                row.host,
                row.running_version,
                row.configured_version,
                row.to_config,
                row.to_restart,
            )
            for row in get_sch_status(db_session)
        ]
    assert set(session_rows) == last_values
