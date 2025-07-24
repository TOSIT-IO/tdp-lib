# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from pathlib import Path
from typing import Callable

import pytest
from click.testing import CliRunner, Result

from tdp.cli.__main__ import cli
from tdp.core.constants import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
)
from tests.conftest import (
    generate_collection_at_path,
    init_dag_directory,
    init_default_vars_directory,
    init_playbooks_directory,
)


@pytest.fixture
def runner() -> Generator[CliRunner, None, None]:
    """Fixture to provide a Click test runner."""
    runner = CliRunner(env={"TDP_MOCK_DEPLOY": "True"})
    # Run tests in an isolated filesystem to avoid side effects (e.g. local .env file)
    with runner.isolated_filesystem():
        yield runner


@pytest.fixture
def tdp(runner):
    """Fixture to provide a function that invokes the TDP CLI with given arguments."""

    def invoke(args: str) -> Result:
        return runner.invoke(cli, args.split())

    return invoke


@pytest.fixture
def vars(tmp_path) -> Path:
    """Fixture to create a temporary directory for storing variable files."""
    vars_path = tmp_path / "vars"
    vars_path.mkdir(parents=True)
    return vars_path


class CollectionPath:
    """A collection path object that provides methods to create collection directories."""

    def __init__(self, path: Path):
        self.path = path
        generate_collection_at_path(self.path)

    def __str__(self) -> str:
        """Return the path as a string."""
        return str(self.path)

    def init_dag_directory(self, dag: dict[str, list]) -> None:
        """Create and populate the DAG directory with service DAG files."""
        init_dag_directory(self.path / DAG_DIRECTORY_NAME, dag)

    def init_playbooks_directory(self, dag: dict[str, list]) -> None:
        """Create and populate the playbooks directory with operation playbooks."""
        init_playbooks_directory(self.path / PLAYBOOKS_DIRECTORY_NAME, dag)

    def init_default_vars_directory(self, vars: dict[str, dict[str, dict]]) -> None:
        """Create and populate the default vars directory with service variables."""
        init_default_vars_directory(self.path / DEFAULT_VARS_DIRECTORY_NAME, vars)


@pytest.fixture
def collection_path_factory(
    tmp_path,
) -> Generator[Callable[[], CollectionPath], None, None]:
    """Fixture that provides a factory function for creating CollectionPath objects.

    The factory function can be called multiple times within a test to create multiple
    collections. Each call will create a new temporary directory with a unique name.

    Returns:
        A factory function that, when called, returns a new CollectionPath object.

    Example usage:
        def test_something(collection_path_factory):
            collection1 = collection_path_factory()
            collection2 = collection_path_factory()
            # Both collections are separate with unique paths
    """
    collection_counter = 0

    def _create_collection_path() -> CollectionPath:
        nonlocal collection_counter
        collection_counter += 1
        collection_dir = tmp_path / f"collection_{collection_counter}"
        return CollectionPath(collection_dir)

    yield _create_collection_path


@pytest.fixture
def collection_path(collection_path_factory) -> CollectionPath:
    return collection_path_factory()
