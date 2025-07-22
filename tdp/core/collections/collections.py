# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""TDP collections.

TDP services are split accross different collections. A core, extras and observability
collections are provided by the maintainers.

The core collection which contains the core services (hdfs, hive, ....) is mandatory. It
can be extended by other collections.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tdp.core.entities.entity_name import create_entity_name
from tdp.core.entities.operation import (
    DagOperationBuilder,
    ForgedDagOperation,
    OperationName,
    Operations,
    OtherPlaybookOperation,
    Playbook,
)
from tdp.core.inventory_reader import InventoryReader
from tdp.core.variables.schema.service_schema import ServiceSchema

from .collection_reader import CollectionReader

if TYPE_CHECKING:
    from tdp.core.types import PathLike


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CollectionVersion:
    repo: Optional[str]
    galaxy: Optional[str]


class Collections:
    """Concatenation of in use collections."""

    def __init__(self, collections: Iterable[CollectionReader]):
        """Build Collections from a sequence of Collection.

        Ordering of the sequence is what will determine the loading order of the operations.
        An operation can override a previous operation.

        Args:
            collections: Ordered Sequence of Collection object.

        Returns:
            A Collections object."""
        self._collection_readers = list(collections)

        self._playbooks = self._read_playbooks()
        self._operations = self._generate_operations()
        self._default_var_dirs = self._init_default_vars_dirs()
        self._schemas = self._init_schemas()

    @staticmethod
    def from_collection_paths(
        paths: Iterable[PathLike], inventory_reader: Optional[InventoryReader] = None
    ):
        inventory_reader = inventory_reader or InventoryReader()
        collection_readers = [
            CollectionReader.from_path(path, inventory_reader) for path in paths
        ]
        return Collections(collection_readers)

    @property
    def playbooks(self) -> dict[str, Playbook]:
        """Mapping of playbook name to Playbook instance."""
        return self._playbooks

    @property
    def operations(self) -> Operations:
        """Mapping of operation name to Operation instance."""
        return self._operations

    @property
    def default_vars_dirs(self) -> dict[str, Path]:
        """Mapping of collection name to their default vars directory."""
        return self._default_var_dirs

    @property
    def schemas(self) -> dict[str, ServiceSchema]:
        """Mapping of service names with their variable schemas."""
        return self._schemas

    def get_version(self, collection_name: str) -> CollectionVersion:
        for collection in self._collection_readers:
            if collection.name == collection_name:
                return CollectionVersion(
                    repo=collection.read_repository_version(),
                    galaxy=collection.read_galaxy_version(),
                )
        raise ValueError(
            f"Can't access collection's versions. Collection '{collection_name}' is not registered."
        )

    def _read_playbooks(self) -> dict[str, Playbook]:
        playbooks: dict[str, Playbook] = {}
        for collection in self._collection_readers:
            for playbook in collection.read_playbooks():
                if playbook.name in playbooks:
                    logger.debug(
                        f"'{playbook.name}' defined in "
                        f"'{playbooks[playbook.name].collection_name}' "
                        f"is overridden by '{collection.name}'"
                    )
                else:
                    logger.debug(f"Adding playbook '{playbook.path}'")
                playbooks[playbook.name] = playbook
        return playbooks

    def _generate_operations(self) -> Operations:
        # Create DagOperationBuilders to merge dag nodes with the same name
        dag_operation_builders: dict[str, DagOperationBuilder] = {}
        for collection in self._collection_readers:
            for dag_node in collection.read_dag_nodes():
                if dag_node.name in dag_operation_builders:
                    dag_operation_builders[dag_node.name].extends(dag_node)
                else:
                    dag_operation_builders[dag_node.name] = (
                        DagOperationBuilder.from_read_dag_node(
                            dag_node=dag_node,
                            playbook=self._playbooks.get(dag_node.name),
                        )
                    )
        # Generate the operations
        operations = Operations()
        for dag_operation_builder in dag_operation_builders.values():
            # 1. Build the DAG operation from the defined dag nodes
            operation = dag_operation_builder.build()
            operations.add(operation)
            # 2. Forge restart and stop operations from start operations
            if operation.name.action == "start":
                restart_operation_name = OperationName(operation.name.entity, "restart")
                operations.add(
                    ForgedDagOperation.create(
                        operation_name=restart_operation_name,
                        source_operation=operation,
                        playbook=self._playbooks.get(str(restart_operation_name)),
                    )
                )
                stop_operation_name = OperationName(operation.name.entity, "stop")
                operations.add(
                    ForgedDagOperation.create(
                        operation_name=stop_operation_name,
                        source_operation=operation,
                        playbook=self._playbooks.get(str(stop_operation_name)),
                    )
                )
        # 3. Parse the remaining playbooks (that are not part of the DAG) as operations
        for playbook in self._playbooks.values():
            operation_name = OperationName.from_str(playbook.name)
            if operation_name not in operations:
                operations.add(OtherPlaybookOperation(operation_name, playbook))
        return operations

    def _init_default_vars_dirs(self) -> dict[str, Path]:
        """Initialize the default vars directories from the collections."""
        default_var_dirs: dict[str, Path] = {}
        for collection in self._collection_readers:
            default_var_dirs[collection.name] = collection.default_vars_directory
        return default_var_dirs

    def _init_schemas(self) -> dict[str, ServiceSchema]:
        """Initialize the variables schemas from the collections."""
        schemas: dict[str, ServiceSchema] = {}
        for collection in self._collection_readers:
            for schema in collection.read_schemas():
                schemas.setdefault(schema.service, ServiceSchema()).add_schema(schema)
        return schemas

    def validate_service_component(
        self, service: str, component: Optional[str]
    ) -> None:
        """Validate that the service and component are registered in the collections."""
        entity_name = create_entity_name(service, component)
        if not entity_name in [op.name.entity for op in self.operations.values()]:
            raise ValueError(
                f"Entity '{entity_name}' does not exist in the collections."
            )
