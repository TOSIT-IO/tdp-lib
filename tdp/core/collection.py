# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

DAG_DIRECTORY_NAME = "tdp_lib_dag"
DEFAULT_VARS_DIRECTORY_NAME = "tdp_vars_defaults"
OPERATION_DIRECTORY_NAME = "playbooks"
SCHEMA_VARS_DIRECTORY_NAME = "tdp_vars_schema"

JSON_EXTENSION = ".json"
YML_EXTENSION = ".yml"

MANDATORY_DIRECTORIES = [
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    OPERATION_DIRECTORY_NAME,
]


class PathDoesNotExistsError(Exception):
    pass


class PathIsNotADirectoryError(Exception):
    pass


class MissingMandatoryDirectoryError(Exception):
    pass


class Collection:
    """An enriched version of an Ansible collection.

    A TDP Collection is a directory containing playbooks, DAGs, default variables and schema variables.

    Attributes:
        path: Path to the collection.
        name: Name of the collection.
        dag_directory: Path to the DAG directory.
        default_vars_directory: Path to the default variables directory.
        operations_directory: Path to the operations directory.
        schema_directory: Path to the schema variables directory.
        dag_yamls: List of DAG files in the YAML format.
        operations: Dictionary of operations with the operation name as key and the operation path as value.
    """

    def __init__(self, path: Union[str, os.PathLike]):
        """Initialize a collection from a path.

        Args:
            path: The path to the collection.
        """
        self._path = Path(path)
        self._dag_yamls: Optional[List[Path]] = None
        self._operations: Optional[Dict[str, Path]] = None

    @staticmethod
    def from_path(path: Union[str, os.PathLike]):
        """Factory method to build a collection from a path.

        Args:
            path: The path to the collection.

        Returns:
            A collection.

        Raises:
            PathDoesNotExistsError: If the path does not exists.
            PathIsNotADirectoryError: If the path is not a directory.
            MissingMandatoryDirectoryError: If the path does not contain the mandatory directories.
        """
        path = Path(path).expanduser().resolve()
        if not path.exists():
            raise PathDoesNotExistsError(f"{path} does not exists")
        if not path.is_dir():
            raise PathIsNotADirectoryError(f"{path} is not a directory")
        for mandatory_directory in MANDATORY_DIRECTORIES:
            mandatory_path = path / mandatory_directory
            if not mandatory_path.exists() or not mandatory_path.is_dir():
                raise MissingMandatoryDirectoryError(
                    f"{path} does not contain the mandatory directory {mandatory_directory}",
                )
        return Collection(path)

    @property
    def path(self) -> Path:
        """Path to the collection."""
        return self._path

    @property
    def name(self) -> str:
        """Name of the collection."""
        return self._path.name

    @property
    def dag_directory(self) -> Path:
        """Path to the DAG directory."""
        return self._path / DAG_DIRECTORY_NAME

    @property
    def default_vars_directory(self) -> Path:
        """Path to the default variables directory."""
        return self._path / DEFAULT_VARS_DIRECTORY_NAME

    @property
    def operations_directory(self) -> Path:
        """Path to the operations directory."""
        return self._path / OPERATION_DIRECTORY_NAME

    @property
    def schema_directory(self) -> Path:
        """Path to the schema variables directory."""
        return self._path / SCHEMA_VARS_DIRECTORY_NAME

    @property
    def dag_yamls(self) -> List[Path]:
        """List of DAG files in the YAML format."""
        if not self._dag_yamls:
            self._dag_yamls = list(self.dag_directory.glob("*" + YML_EXTENSION))
        return self._dag_yamls

    @property
    def operations(self) -> Dict[str, Path]:
        """Dictionary of operations (playbooks) by name."""
        if not self._operations:
            self._operations = {
                playbook.stem: playbook
                for playbook in self.operations_directory.glob("*" + YML_EXTENSION)
            }
        return self._operations

    def get_service_default_vars(self, service_name: str) -> List[Path]:
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
        """Get the schema of the service.

        Args:
            service_name: The name of the service.

        Returns:
            The schema of the service's variables.
        """
        schema_path = self.schema_directory / (service_name + JSON_EXTENSION)
        if not schema_path.exists():
            return {}
        with schema_path.open() as fd:
            return json.load(fd)
