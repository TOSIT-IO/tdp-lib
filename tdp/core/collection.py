# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import json
from pathlib import Path
from typing import Optional

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

MANDATORY_DIRECTORIES = [
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    PLAYBOOKS_DIRECTORY_NAME,
]


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
        self._dag_yamls: Optional[list[Path]] = None
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
    def dag_yamls(self) -> list[Path]:
        """List of DAG files in the YAML format."""
        if not self._dag_yamls:
            self._dag_yamls = list(self.dag_directory.glob("*" + YML_EXTENSION))
        return self._dag_yamls

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

    def get_service_schema(self, service_name: str) -> dict:
        """Get the variables schema of a service.

        Args: Name of the service to retrieve the schema for.

        Returns: The service schema.
        """
        schema_path = self.schema_directory / (service_name + JSON_EXTENSION)
        if not schema_path.exists():
            return {}
        with schema_path.open() as fd:
            return json.load(fd)

    def _check_path(self):
        """Validate the collection path content."""
        if not self._path.exists():
            raise PathDoesNotExistsError(f"{self._path} does not exists.")
        if not self._path.is_dir():
            raise PathIsNotADirectoryError(f"{self._path} is not a directory.")
        for mandatory_directory in MANDATORY_DIRECTORIES:
            mandatory_path = self._path / mandatory_directory
            if not mandatory_path.exists() or not mandatory_path.is_dir():
                raise MissingMandatoryDirectoryError(
                    f"{self._path} does not contain the mandatory directory {mandatory_directory}.",
                )


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
