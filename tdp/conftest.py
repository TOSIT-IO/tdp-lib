# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from pathlib import Path

import pytest
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from tdp.core.constants import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    OPERATION_SLEEP_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
    YML_EXTENSION,
)
from tdp.core.models import BaseModel

DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture()
def db_session() -> Generator[Session, None, None]:
    # Connect to the database
    engine = create_engine(DATABASE_URL)

    # Create tables
    BaseModel.metadata.create_all(engine)

    # Create a session
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    yield session

    # Close and rollback for isolation
    session.close()
    BaseModel.metadata.drop_all(engine)
    engine.dispose()


def generate_collection_at_path(
    path: Path,
    dag: dict[str, list],
    vars: dict[str, dict[str, dict]],
) -> None:
    """Generate a collection at a given path."""
    (dag_dir := path / DAG_DIRECTORY_NAME).mkdir()
    (playbooks_dir := path / PLAYBOOKS_DIRECTORY_NAME).mkdir()
    (tdp_vars_defaults_dir := path / DEFAULT_VARS_DIRECTORY_NAME).mkdir()

    # Minimal playbook which will be used for operations
    minimal_playbook = [
        {"hosts": "localhost"},
    ]

    for service_name, operations in dag.items():
        # Save the dag
        with (dag_dir / (service_name + YML_EXTENSION)).open("w") as fd:
            yaml.dump(operations, fd)

        # Save playbooks
        for operation in operations:
            # Generate and save a restart playbook for each start operation
            if operation["name"].endswith("_start") and "noop" not in operation:
                with (
                    playbooks_dir
                    / (operation["name"].rstrip("_start") + "_restart" + YML_EXTENSION)
                ).open("w") as fd:
                    yaml.dump(minimal_playbook, fd)
            # Save the playbook
            with (playbooks_dir / (operation["name"] + YML_EXTENSION)).open("w") as fd:
                yaml.dump(minimal_playbook, fd)

    # Save the sleep playbook
    with (playbooks_dir / (OPERATION_SLEEP_NAME + YML_EXTENSION)).open("w") as fd:
        yaml.dump(minimal_playbook, fd)

    # Save the vars
    for service_name, file_vars in vars.items():
        service_dir = tdp_vars_defaults_dir / service_name
        service_dir.mkdir()
        for filename, vars in file_vars.items():
            with (service_dir / filename).open("w") as fd:
                yaml.dump(vars, fd)
