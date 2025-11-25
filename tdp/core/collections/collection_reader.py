# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import logging
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from tdp.core.collections.playbook_validate import validate_playbook
from tdp.core.constants import (
    DAG_DIRECTORY_NAME,
    DEFAULT_VARS_DIRECTORY_NAME,
    JSON_EXTENSION,
    PLAYBOOKS_DIRECTORY_NAME,
    SCHEMA_VARS_DIRECTORY_NAME,
    YML_EXTENSION,
)
from tdp.core.entities.operation import Playbook, PlaybookMeta
from tdp.core.inventory_reader import InventoryReader
from tdp.core.repository.utils.get_repository_version import get_repository_version
from tdp.core.types import PathLike
from tdp.core.variables.schema import ServiceCollectionSchema
from tdp.core.variables.schema.exceptions import InvalidSchemaError

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

if TYPE_CHECKING:
    from tdp.core.collections.playbook_validate import PlaybookIn

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
    """Model for a DAG node read from a DAG file.

    Meant to be used in a DagNodeBuilder.

    Args:
        name: Name of the operation.
        depends_on: List of operations that must be executed before this one.
        noop: Whether the operation is a noop.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    name: str
    depends_on: frozenset[str] = frozenset()
    noop: Optional[bool] = False


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
        inventory_reader: InventoryReader,
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
        self._inventory_reader = inventory_reader

    # ? Is this method really useful?
    @staticmethod
    def from_path(
        path: PathLike,
        inventory_reader: InventoryReader,
    ) -> CollectionReader:
        """Factory method to create a collection from a path.

        Args:
            path: The path to the collection.

        Returns: A collection.
        """
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
            playbook: PlaybookIn = validate_playbook(playbook_path)
            yield Playbook(
                path=playbook_path,
                collection_name=self.name,
                hosts=self._inventory_reader.get_hosts_from_playbook(playbook),
                meta=_get_playbook_meta(playbook, playbook_path),
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

    def read_galaxy_version(self) -> Optional[str]:
        return _get_galaxy_version(self._path)

    def read_repository_version(self) -> Optional[str]:
        return get_repository_version(self.path)

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


def _get_playbook_meta(playbook: PlaybookIn, playbook_path: Path) -> PlaybookMeta:
    can_limit = True
    can_limit_true_plays = list[str]()

    for play_nb, play in enumerate(playbook):
        play_name = f"{play.name}[{play_nb}]" if play.name else f"play[{play_nb}]"
        if vars := play.vars:
            if tdp_lib := vars.tdp_lib:
                if tdp_lib.can_limit == True:
                    can_limit_true_plays.append(play_name)
                elif can_limit == True and tdp_lib.can_limit == False:
                    can_limit = False

    if can_limit == False and len(can_limit_true_plays) > 0:
        logger.warning(
            f"Playbook '{playbook_path}': tdp_lib.can_limit is both true and false "
            "accross plays. Because a play sets 'can_limit: false', the playbook "
            "can_limit is false; the 'can_limit: true' flags on these plays are "
            "ignored: " + ", ".join(can_limit_true_plays)
        )

    return PlaybookMeta(can_limit=can_limit)


def _get_galaxy_version(
    path: Path,
) -> Optional[str]:
    """Read the galaxy version from MANIFEST.json file in the collection root.

    Returns:
        The galaxy version if it exists, otherwise None.
    """
    try:
        manifest_path = path / "MANIFEST.json"
        with manifest_path.open("r") as fd:
            manifest = json.load(fd)
            return manifest["collection_info"]["version"]
    except FileNotFoundError:
        pass
    except json.JSONDecodeError:
        _log_get_galaxy_version_error(f"can't parse ${manifest_path}.")
    except KeyError:
        _log_get_galaxy_version_error(
            f"'collection_info.version' not found in ${manifest_path}."
        )
    except Exception as e:
        _log_get_galaxy_version_error(str(e))
    return None


def _log_get_galaxy_version_error(msg: str) -> None:
    logger.error("Error while reading ansible galaxy version: " + msg)
