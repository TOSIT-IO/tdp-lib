# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from tdp.core.constants import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    OPERATION_SLEEP_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
    YML_EXTENSION,
)
from tdp.core.models import BaseModel, init_database


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--database-dsn",
        dest="database_dsn",
        action="append",
        default=["sqlite"],
        help="Add database DSN.",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Pytest hook to generate tests based on the database dsn option."""
    if "db_dsn" in metafunc.fixturenames:
        metafunc.parametrize(
            "db_dsn",
            metafunc.config.getoption("database_dsn"),
            indirect=True,  # type: ignore
        )


@pytest.fixture
def db_dsn(
    request: pytest.FixtureRequest, tmp_path: Path
) -> Generator[str, None, None]:
    """Return a database dsn.

    Ensure that the database is cleaned up after each test is done.

    We create a temp path instead of the default in-memory sqlite database as some test
    need to generate several engine instances (which will loose the data between them).
    Concerned tests are CLI tests that need to perform a `tdp init` at the beginning of
    the test.
    """
    database_dsn = cast(str, request.param)
    # Assign a temporary database for sqlite
    if database_dsn == "sqlite":
        database_dsn = f"sqlite:///{tmp_path / 'test.db'}"
    yield database_dsn


@pytest.fixture()
def db_engine(
    db_dsn: str, request: pytest.FixtureRequest
) -> Generator[Engine, None, None]:
    """Create a database engine and optionnally by default initialize the schema."""
    engine = create_engine(db_dsn)
    if request.param:
        init_database(engine)
    yield engine
    if request.param:
        BaseModel.metadata.drop_all(engine)
    engine.dispose()


@contextmanager
def create_session(engine: Engine) -> Generator[Session, None, None]:
    """Utility function to create a session."""
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()


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
            # Do not generate playbooks for noop operations
            if "noop" in operation:
                continue
            # Generate and save stop and restart playbooks for each start operation
            if operation["name"].endswith("_start"):
                with (
                    playbooks_dir
                    / (operation["name"].rstrip("_start") + "_restart" + YML_EXTENSION)
                ).open("w") as fd:
                    yaml.dump(minimal_playbook, fd)
                with (
                    playbooks_dir
                    / (operation["name"].rstrip("_start") + "_stop" + YML_EXTENSION)
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


def assert_equal_values_in_model(model1: Any, model2: Any) -> bool:
    """SQLAlchemy asserts that two identical objects of type DeclarativeBase parent of the BaseModel class,
    which is used in TDP as pattern for the table models, are identical if they are compared in the same session,
    but different if compared in two different sessions.

    This function therefore transforms the tables into dictionaries and by parsing the coulumns compares their values.
    """
    if isinstance(model1, BaseModel) and isinstance(model2, BaseModel):
        return model1.to_dict() == model2.to_dict()
    else:
        return False
