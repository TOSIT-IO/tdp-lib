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
from tdp.core.variables.schema.exceptions import InvalidSchemaError

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


class CollectionReader:
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

    # ? Is this method really useful?
    @staticmethod
    def from_path(
        path: PathLike,
        inventory_reader: Optional[InventoryReader] = None,
    ) -> CollectionReader:
        """Factory method to create a collection from a path.

        Args:
            path: The path to the collection.

        Returns: A collection.
        """
        inventory_reader = inventory_reader or InventoryReader()
        return CollectionReader(Path(path).expanduser().resolve(), inventory_reader)

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

    def read_dag_nodes(self) -> Generator[TDPLibDagNodeModel, None, None]:
        """Read the DAG nodes stored in the dag_directory."""
        return read_dag_directory(self.dag_directory)

    def read_playbooks(self) -> dict[str, Playbook]:
        """Read the playbooks stored in the playbooks_directory."""
        return read_playbooks_directory(
            self.playbooks_directory,
            self.name,
            inventory_reader=self._inventory_reader,
        )

    def read_schemas(self) -> list[ServiceCollectionSchema]:
        """Read the schemas stored in the schema_directory."""
        return read_schema_directory(self.schema_directory)


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


def read_schema_directory(directory_path: Path) -> list[ServiceCollectionSchema]:
    """Read the schemas from a directory.

    This function is meant to be used only once during the initialization of a
    collection object.

    Invalid schemas are ignored.

    Args:
        directory_path: Path to the schema directory.

    Returns:
        Dictionary of schemas.
    """
    schemas: list[ServiceCollectionSchema] = []
    for schema_path in (directory_path).glob("*" + JSON_EXTENSION):
        try:
            schemas.append(ServiceCollectionSchema.from_path(schema_path))
        except InvalidSchemaError as e:
            logger.warning(f"{e}. Ignoring schema.")
    return schemas


def read_playbooks_directory(
    directory_path: Path,
    collection_name: str,
    inventory_reader: Optional[InventoryReader] = None,
) -> dict[str, Playbook]:
    """Read the playbooks from a directory.

    This function is meant to be used only once during the initialization of a
    collection object.

    Args:
        directory_path: Path to the playbooks directory.
        collection_name: Name of the collection.
        inventory_reader: Inventory reader.

    Returns:
        Dictionary of playbooks.
    """
    return {
        playbook_path.stem: Playbook(
            playbook_path,
            collection_name,
            read_hosts_from_playbook(playbook_path, inventory_reader),
        )
        for playbook_path in (directory_path).glob("*" + YML_EXTENSION)
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


def read_dag_directory(
    directory_path: Path,
) -> Generator[TDPLibDagNodeModel, None, None]:
    """Read the DAG files from a directory.

    Args:
        directory_path: Path to the DAG directory.

    Returns:
        List of DAG nodes.
    """
    for dag_file in (directory_path).glob("*" + YML_EXTENSION):
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
