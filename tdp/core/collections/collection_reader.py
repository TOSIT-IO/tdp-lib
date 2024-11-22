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


class TDPLibDagNodeModel(BaseModel):
    """Model for a TDP operation defined in a tdp_lib_dag file."""

    model_config = ConfigDict(extra="ignore")

    name: str
    depends_on: list[str] = []


class TDPLibDagModel(BaseModel):
    """Model for a TDP DAG defined in a tdp_lib_dag file."""

    model_config = ConfigDict(extra="ignore")

    operations: list[TDPLibDagNodeModel]


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
        self._check_collection_structure(self._path)
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
        for dag_file in (self.dag_directory).glob("*" + YML_EXTENSION):
            with dag_file.open("r") as operations_file:
                file_content = yaml.load(operations_file, Loader=Loader)

            try:
                tdp_lib_dag = TDPLibDagModel(operations=file_content)
                for operation in tdp_lib_dag.operations:
                    yield operation
            except ValidationError as e:
                logger.error(f"Error while parsing tdp_lib_dag file {dag_file}: {e}")
                raise

    def read_playbooks(self) -> Generator[Playbook, None, None]:
        """Read the playbooks stored in the playbooks_directory."""
        for playbook_path in (self.playbooks_directory).glob("*" + YML_EXTENSION):
            yield Playbook(
                path=playbook_path,
                collection_name=self.name,
                hosts=read_hosts_from_playbook(playbook_path, self._inventory_reader),
            )

    def read_schemas(self) -> list[ServiceCollectionSchema]:
        """Read the schemas stored in the schema_directory.

        Invalid schemas are ignored.
        """
        schemas: list[ServiceCollectionSchema] = []
        for schema_path in (self.schema_directory).glob("*" + JSON_EXTENSION):
            try:
                schemas.append(ServiceCollectionSchema.from_path(schema_path))
            except InvalidSchemaError as e:
                logger.warning(f"{e}. Ignoring schema.")
        return schemas

    def _check_collection_structure(self, path: Path) -> None:
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
