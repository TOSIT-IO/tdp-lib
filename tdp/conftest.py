# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Generator, Mapping

import pytest
import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from tdp.core.collection import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
    SCHEMA_VARS_DIRECTORY_NAME,
)
from tdp.core.collections import OPERATION_SLEEP_NAME
from tdp.core.models import Base

DATABASE_URL = "sqlite:///:memory:"


@pytest.fixture()
def db_session() -> Generator[Session, None, None]:
    # Connect to the database
    engine = create_engine(DATABASE_URL)

    # Create tables
    Base.metadata.create_all(engine)

    # Create a session
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    yield session

    # Close and rollback for isolation
    session.close()
    Base.metadata.drop_all(engine)
    engine.dispose()


def generate_collection(
    directory: Path,
    dag_service_operations: Mapping[str, list],
    service_vars: Mapping[str, Mapping[str, dict]],
) -> None:
    tdp_lib_dag = directory / DAG_DIRECTORY_NAME
    playbooks = directory / PLAYBOOKS_DIRECTORY_NAME
    tdp_vars_defaults = directory / DEFAULT_VARS_DIRECTORY_NAME
    tdp_vars_schema = directory / SCHEMA_VARS_DIRECTORY_NAME

    tdp_lib_dag.mkdir()
    playbooks.mkdir()
    tdp_vars_defaults.mkdir()
    tdp_vars_schema.mkdir()

    minimal_playbook = [
        {"hosts": "localhost"},
    ]

    for service, operations in dag_service_operations.items():
        service_filename = service + ".yml"
        with (tdp_lib_dag / service_filename).open("w") as fd:
            yaml.dump(operations, fd)

        for operation in operations:
            if operation["name"].endswith("_start") and "noop" not in operation:
                with (
                    playbooks / (operation["name"].rstrip("_start") + "_restart.yml")
                ).open("w") as fd:
                    yaml.dump(minimal_playbook, fd)
            with (playbooks / (operation["name"] + ".yml")).open("w") as fd:
                yaml.dump(minimal_playbook, fd)

    with (playbooks / (OPERATION_SLEEP_NAME + ".yml")).open("w") as fd:
        yaml.dump(minimal_playbook, fd)

    for service_name, file_vars in service_vars.items():
        service_dir = tdp_vars_defaults / service_name
        service_dir.mkdir()
        for filename, vars in file_vars.items():
            with (service_dir / filename).open("w") as fd:
                yaml.dump(vars, fd)
