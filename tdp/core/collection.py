# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Generator
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from tdp.core.constants import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    JSON_EXTENSION,
    PLAYBOOKS_DIRECTORY_NAME,
    SCHEMA_VARS_DIRECTORY_NAME,
    YML_EXTENSION,
)
from tdp.core.entities.operation import Playbook
from tdp.core.inventory_reader import InventoryReader
from tdp.core.types import PathLike
from tdp.core.variables.schema import ServiceCollectionSchema

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

MANDATORY_DIRECTORIES = [
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
]

logger = logging.getLogger(__name__)


class PathDoesNotExistsError(Exception):
    pass


class PathIsNotADirectoryError(Exception):
    pass


class MissingMandatoryDirectoryError(Exception):
    pass


class Collection:
    """An enriched version of an Ansible collection.

    A TDP Collection is a directory containing playbooks, DAGs, default variables and variables schemas.
    """

    def __init__(
        self,
        path: PathLike,
        inventory_reader: Optional[InventoryReader] = None,
    ):
        """Initialize a collection.

        Args:
            path: The path to the collection.

        Raises:
            PathDoesNotExistsError: If the path does not exists.
            PathIsNotADirectoryError: If the path is not a directory.
            MissingMandatoryDirectoryError: If the collection does not contain a mandatory directory.
        """
        self._path = Path(path)
        check_collection_structure(self._path)

        self._inventory_reader = inventory_reader or InventoryReader()
        self._dag_nodes = list(get_collection_dag_nodes(self._path))
        self._playbooks = get_collection_playbooks(
            self._path,
            inventory_reader=self._inventory_reader,
        )

    @staticmethod
    def from_path(path: PathLike) -> "Collection":
        """Factory method to create a collection from a path.

        Args:
            path: The path to the collection.

        Returns: A collection.
        """
        return Collection(path=Path(path).expanduser().resolve())

    @property
    def name(self) -> str:
        """Collection name."""
        return self._path.name

    @property
    def path(self) -> Path:
        """Path to the collection."""
        return self._path

    @property
    def dag_directory(self) -> Path:
        """Path to the DAG directory."""
        return self._path / DAG_DIRECTORY_NAME

    @property
    def default_vars_directory(self) -> Path:
        """Path to the default variables directory."""
        return self._path / DEFAULT_VARS_DIRECTORY_NAME

    @property
    def playbooks_directory(self) -> Path:
        """Path to the playbook directory."""
        return self._path / PLAYBOOKS_DIRECTORY_NAME

    @property
    def schema_directory(self) -> Path:
        """Path to the variables schema directory."""
        return self._path / SCHEMA_VARS_DIRECTORY_NAME

    @property
    def dag_nodes(self) -> list[TDPLibDagNodeModel]:
        """List of DAG files in the YAML format."""
        return self._dag_nodes  # TODO: should return a generator

    @property
    def playbooks(self) -> dict[str, Playbook]:
        """Dictionary of playbooks."""
        return self._playbooks

    def get_service_default_vars(self, service_name: str) -> list[tuple[str, Path]]:
        """Get the default variables for a service.

        Args:
            service_name: The name of the service.

        Returns:
            A list of tuples (name, path) of the default variables.
        """
        service_path = self.default_vars_directory / service_name
        if not service_path.exists():
            return []
        return [(path.name, path) for path in service_path.glob("*" + YML_EXTENSION)]

    def get_service_schema(self, service_name: str) -> ServiceCollectionSchema:
        """Get the variables schema of a service.

        Args:
            Name of the service to retrieve the schema for.

        Returns:
            The service schema.

        Raises:
            InvalidSchemaError: If the schema is not a dict or a bool.
            SchemaNotFoundError: If the schema is not found.
        """
        schema_path = self.schema_directory / (service_name + JSON_EXTENSION)
        return ServiceCollectionSchema.from_path(schema_path)


def check_collection_structure(path: Path) -> None:
    """Check the structure of a collection.

    Args:
        path: Path to the collection.

    Raises:
        PathDoesNotExistsError: If the path does not exists.
        PathIsNotADirectoryError: If the path is not a directory.
        MissingMandatoryDirectoryError: If the collection does not contain a mandatory directory.
    """
    if not path.exists():
        raise PathDoesNotExistsError(f"{path} does not exists.")
    if not path.is_dir():
        raise PathIsNotADirectoryError(f"{path} is not a directory.")
    for mandatory_directory in MANDATORY_DIRECTORIES:
        mandatory_path = path / mandatory_directory
        if not mandatory_path.exists() or not mandatory_path.is_dir():
            raise MissingMandatoryDirectoryError(
                f"{path} does not contain the mandatory directory {mandatory_directory}.",
            )


def get_collection_playbooks(
    collection_path: Path,
    playbooks_directory_name=PLAYBOOKS_DIRECTORY_NAME,
    inventory_reader: Optional[InventoryReader] = None,
) -> dict[str, Playbook]:
    """Get the playbooks of a collection.

    This function is meant to be used only once during the initialization of a
    collection object.

    Args:
        collection_path: Path to the collection.
        playbook_directory: Name of the playbook directory.
        inventory_reader: Inventory reader.

    Returns:
        Dictionary of playbooks.
    """
    return {
        playbook_path.stem: Playbook(
            playbook_path,
            collection_path.name,
            read_hosts_from_playbook(playbook_path, inventory_reader),
        )
        for playbook_path in (collection_path / playbooks_directory_name).glob(
            "*" + YML_EXTENSION
        )
    }


def read_hosts_from_playbook(
    playbook_path: Path, inventory_reader: Optional[InventoryReader]
) -> set[str]:
    """Read the hosts from a playbook.

    Args:
        playbook_path: Path to the playbook.
        inventory_reader: Inventory reader.

    Returns:
        Set of hosts.
    """
    if not inventory_reader:
        inventory_reader = InventoryReader()
    try:
        with playbook_path.open() as fd:
            return inventory_reader.get_hosts_from_playbook(fd)
    except Exception as e:
        raise ValueError(f"Can't parse playbook {playbook_path}.") from e


def get_collection_dag_nodes(
    collection_path: Path, dag_directory_name=DAG_DIRECTORY_NAME
) -> Generator[TDPLibDagNodeModel, None, None]:
    """Get the DAG nodes of a collection.

    Args:
        collection_path: Path to the collection.
        dag_directory_name: Name of the DAG directory.

    Returns:
        List of DAG nodes.
    """
    for dag_file in (collection_path / dag_directory_name).glob("*" + YML_EXTENSION):
        yield from read_dag_file(dag_file)


class TDPLibDagNodeModel(BaseModel):
    """Model for a TDP operation defined in a tdp_lib_dag file."""

    model_config = ConfigDict(extra="ignore")

    name: str
    depends_on: list[str] = []


class TDPLibDagModel(BaseModel):
    """Model for a TDP DAG defined in a tdp_lib_dag file."""

    model_config = ConfigDict(extra="ignore")

    operations: list[TDPLibDagNodeModel]


def read_dag_file(
    dag_file_path: Path,
) -> Generator[TDPLibDagNodeModel, None, None]:
    """Read a tdp_lib_dag file and return a list of DAG operations."""
    with dag_file_path.open("r") as operations_file:
        file_content = yaml.load(operations_file, Loader=Loader)

    try:
        tdp_lib_dag = TDPLibDagModel(operations=file_content)
        for operation in tdp_lib_dag.operations:
            yield operation
    except ValidationError as e:
        logger.error(f"Error while parsing tdp_lib_dag file {dag_file_path}: {e}")
        raise
