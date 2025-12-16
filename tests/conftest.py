# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from pathlib import Path
from typing import Optional, cast

import pytest
import yaml

from tdp.core.constants import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    OPERATION_SLEEP_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
    YML_EXTENSION,
)


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add custom command-line options for pytest.

    This function adds the --database-dsn option that allows specifying multiple
    database data source names (DSNs) for testing. The option can be used multiple
    times to test against different database backends. The default value is "sqlite",
    it will always be present in the list of DSNs.

    Usage examples:
        # Test only with sqlite (default behavior)
        pytest tests
        # The resulting list will be: ["sqlite"]

        # Test with sqlite and postgresql
        pytest tests --database-dsn postgresql://user:pass@localhost/testdb
        # The resulting list will be: ["sqlite", "postgresql://user:pass@localhost/testdb"]
    """
    parser.addoption(
        "--database-dsn",
        action="append",
        default=["sqlite"],
        help="Add database DSN.",
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Pytest hook to generate tests based on the --database-dsn option."""
    if "db_dsn" in metafunc.fixturenames:
        database_dsns = cast(list, metafunc.config.getoption("--database-dsn"))
        metafunc.parametrize(
            "db_dsn",
            database_dsns,
            indirect=True,
        )


@pytest.fixture
def db_dsn(request, tmp_path) -> Generator[str, None, None]:
    """Return a database dsn.

    Create a temp path instead of the default in-memory sqlite database as some test
    need to generate several engine instances (which will loose the data between them).
    Concerned tests are CLI tests that need to perform a `tdp init` at the beginning of
    the test.
    """
    database_dsn = cast(str, request.param)
    # Assign a temporary database for sqlite
    if database_dsn == "sqlite":
        database_dsn = f"sqlite:///{tmp_path / 'test.db'}"

    yield database_dsn


def init_dag_directory(path: Path, dag: dict[str, list]) -> None:
    """Create and populate the DAG directory with service DAG files."""
    for service_name, operations in dag.items():
        # Save the dag
        with (path / (service_name + YML_EXTENSION)).open("w") as fd:
            yaml.dump(operations, fd)


def init_playbooks_directory(path: Path, dag: dict[str, list]) -> None:
    """Create and populate the playbooks directory with operation playbooks."""
    # Minimal playbook which will be used for operations
    minimal_playbook = [
        {"hosts": "localhost"},
    ]

    for service_name, operations in dag.items():
        # Save playbooks
        for operation in operations:
            # Do not generate playbooks for noop operations
            if "noop" in operation:
                continue
            # Generate and save stop and restart playbooks for each start operation
            if operation["name"].endswith("_start"):
                with (
                    path
                    / (operation["name"].rstrip("_start") + "_restart" + YML_EXTENSION)
                ).open("w") as fd:
                    yaml.dump(minimal_playbook, fd)
                with (
                    path
                    / (operation["name"].rstrip("_start") + "_stop" + YML_EXTENSION)
                ).open("w") as fd:
                    yaml.dump(minimal_playbook, fd)
            # Save the playbook
            with (path / (operation["name"] + YML_EXTENSION)).open("w") as fd:
                yaml.dump(minimal_playbook, fd)

    # Save the sleep playbook
    with (path / (OPERATION_SLEEP_NAME + YML_EXTENSION)).open("w") as fd:
        yaml.dump(minimal_playbook, fd)


def init_default_vars_directory(path: Path, vars: dict[str, dict[str, dict]]) -> None:
    """Create and populate the default vars directory with service variables."""
    for service_name, file_vars in vars.items():
        service_dir = path / service_name
        service_dir.mkdir()
        for filename, vars in file_vars.items():
            if not filename.endswith(YML_EXTENSION):
                filename += YML_EXTENSION
            with (service_dir / filename).open("w") as fd:
                yaml.dump(vars, fd, sort_keys=False)


def generate_collection_at_path(
    path: Path,
    dag: Optional[dict[str, list]] = None,
    vars: Optional[dict[str, dict[str, dict]]] = None,
) -> Path:
    """Generate a collection at a given path."""
    path.mkdir(parents=True, exist_ok=True)

    # Dag
    (dag_dir := path / DAG_DIRECTORY_NAME).mkdir(parents=True)
    if dag:
        init_dag_directory(dag_dir, dag)

    # Playbooks
    (playbooks_dir := path / PLAYBOOKS_DIRECTORY_NAME).mkdir()
    if dag:
        init_playbooks_directory(playbooks_dir, dag)

    # Default vars
    (tdp_vars_defaults_dir := path / DEFAULT_VARS_DIRECTORY_NAME).mkdir(parents=True)
    if vars:
        init_default_vars_directory(tdp_vars_defaults_dir, vars)
    return path
