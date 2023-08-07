# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

from tdp.core.inventory_reader import InventoryReader

DAG_DIRECTORY_NAME = "tdp_lib_dag"
DEFAULT_VARS_DIRECTORY_NAME = "tdp_vars_defaults"
PLAYBOOKS_DIRECTORY_NAME = "playbooks"
SCHEMA_VARS_DIRECTORY_NAME = "tdp_vars_schema"

JSON_EXTENSION = ".json"
YML_EXTENSION = ".yml"

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


class MissingPlaybookError(Exception):
    pass


class Collection:
    """An enriched version of an Ansible collection.

    A TDP Collection is a directory containing playbooks, DAGs, default variables and variables schemas.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike],
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
        self._check_path()

        self._inventory_reader = inventory_reader or InventoryReader()
        self._dag_yamls: Optional[List[Path]] = None
        self._playbooks: Optional[Dict[str, Path]] = None

    @staticmethod
    def from_path(path: Union[str, os.PathLike]) -> "Collection":
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
    def dag_yamls(self) -> List[Path]:
        """List of DAG files in the YAML format."""
        if not self._dag_yamls:
            self._dag_yamls = list(self.dag_directory.glob("*" + YML_EXTENSION))
        return self._dag_yamls

    @property
    def playbooks(self) -> Dict[str, Path]:
        """Dictionary of playbooks."""
        if not self._playbooks:
            self._playbooks = {
                playbook.stem: playbook
                for playbook in self.playbooks_directory.glob("*" + YML_EXTENSION)
            }
        return self._playbooks

    def get_service_default_vars(self, service_name: str) -> List[Tuple[str, Path]]:
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

    def get_service_schema(self, service_name: str) -> Dict:
        """Get the variables schema of a service.

        Args: Name of the service to retrieve the schema for.

        Returns: The service schema.
        """
        schema_path = self.schema_directory / (service_name + JSON_EXTENSION)
        if not schema_path.exists():
            return {}
        with schema_path.open() as fd:
            return json.load(fd)

    def get_hosts_from_playbook(self, playbook: str) -> Set[str]:
        """Get the set of hosts for a playbook.

        Args:
            playbook: playbook name without extension

        Returns:
            Set of hosts
        """
        if playbook not in self.playbooks:
            raise MissingPlaybookError(f"Playbook {playbook} not found")
        try:
            return self._inventory_reader.get_hosts_from_playbook(
                self.playbooks[playbook].open()
            )
        except Exception as e:
            raise ValueError(f"Can't parse playbook {self.playbooks[playbook]}") from e

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
                    f"{self._path} does not contain the mandatory directory {mandatory_directory}",
                )
