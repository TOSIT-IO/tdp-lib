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
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tdp.core.entities.entity_name import ServiceComponentName
from tdp.core.entities.operation import Operation, Operations, Playbook
from tdp.core.inventory_reader import InventoryReader
from tdp.core.variables.schema.service_schema import ServiceSchema

from .collection_reader import CollectionReader

if TYPE_CHECKING:
    from tdp.core.types import PathLike


logger = logging.getLogger(__name__)


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

        self._init_playbooks()
        self._dag_operations, self._other_operations = self._init_operations()
        self._default_var_directories = self._init_default_vars_dirs()
        self._schemas = self._init_schemas()
        self._services_components = self._init_entities()

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
    def dag_operations(self) -> Operations:
        """Mapping of operation name that are defined in dag files to their Operation instance."""
        return self._dag_operations

    @property
    def other_operations(self) -> Operations:
        """Mapping of operation name that aren't in dag files to their Operation instance."""
        return self._other_operations

    @property
    def operations(self) -> Operations:
        """Mapping of all operation name to Operation instance."""
        operations = Operations()
        if self._dag_operations:
            operations.update(self._dag_operations)
        if self._other_operations:
            operations.update(self._other_operations)
        return operations

    @property
    def playbooks(self) -> dict[str, Playbook]:
        """Mapping of playbook name to Playbook instance."""
        return self._playbooks

    @property
    def default_vars_dirs(self) -> dict[str, Path]:
        """Mapping of collection name to their default vars directory."""
        return self._default_var_directories

    @property
    def schemas(self) -> dict[str, ServiceSchema]:
        """Mapping of service with their variable schemas."""
        return self._schemas

    # ? The mapping is using service name as a string for convenience. Should we keep
    # ? this or change it to ServiceName?
    @property
    def entities(self) -> dict[str, set[ServiceComponentName]]:
        """Mapping of services to their set of components."""
        return self._services_components

    def _init_default_vars_dirs(self) -> dict[str, Path]:
        """Mapping of collection name to their default vars directory."""
        default_var_directories = {}
        for collection in self._collection_readers:
            default_var_directories[collection.name] = collection.default_vars_directory
        return default_var_directories

    def _init_playbooks(self) -> None:
        """Initialize the playbooks from the collections.

        If a playbook is defined in multiple collections, the last one will take
        precedence over the previous ones.
        """
        logger.debug("Initializing playbooks")
        self._playbooks: dict[str, Playbook] = {}
        for collection in self._collection_readers:
            for playbook in collection.read_playbooks():
                if playbook.path.stem in self._playbooks:
                    logger.debug(
                        f"'{playbook.name}' defined in "
                        f"'{self._playbooks[playbook.name].collection_name}' "
                        f"is overridden by '{collection.name}'"
                    )
                else:
                    logger.debug(f"Adding playbook '{playbook.path}'")
                self._playbooks[playbook.name] = playbook
        logger.debug("Playbooks initialized")

    def _init_operations(self) -> tuple[Operations, Operations]:
        dag_operations = Operations()
        other_operations = Operations()

        for collection in self._collection_readers:
            # Load DAG operations from the dag files
            for dag_node in collection.read_dag_nodes():
                existing_operation = dag_operations.get(dag_node.name.name)

                # The read_operation is associated with a playbook defined in the
                # current collection
                if playbook := self.playbooks.get(dag_node.name.name):
                    # TODO: would be nice to dissociate the Operation class from the playbook and store the playbook in the Operation
                    dag_operation_to_register = Operation(
                        name=dag_node.name.name,
                        collection_name=collection.name,
                        host_names=playbook.hosts,  # TODO: do not store the hosts in the Operation object
                        depends_on=list(dag_node.depends_on),
                    )
                    # If the operation is already registered, merge its dependencies
                    if existing_operation:
                        dag_operation_to_register.depends_on.extend(
                            dag_operations[dag_node.name.name].depends_on
                        )
                    # Register the operation
                    dag_operations[dag_node.name.name] = dag_operation_to_register
                    continue

                # The read_operation is already registered
                if existing_operation:
                    logger.debug(
                        f"'{dag_node.name}' defined in "
                        f"'{existing_operation.collection_name}' "
                        f"is extended by '{collection.name}'"
                    )
                    existing_operation.depends_on.extend(dag_node.depends_on)
                    continue

                # From this point, the read_operation is a noop as it is not defined
                # in the current nor the previous collections

                # Create and register the operation
                dag_operations[dag_node.name.name] = Operation(
                    name=dag_node.name.name,
                    collection_name=collection.name,
                    depends_on=list(dag_node.depends_on),
                    noop=True,
                    host_names=None,
                )
                # 'restart' and 'stop' operations are not defined in the DAG file
                # for noop, they need to be generated from the start operations
                if dag_node.name.name.endswith("_start"):
                    logger.debug(
                        f"'{dag_node.name}' is noop, creating the associated "
                        "restart and stop operations"
                    )
                    # Create and register the restart operation
                    restart_operation_name = dag_node.name.name.replace(
                        "_start", "_restart"
                    )
                    other_operations[restart_operation_name] = Operation(
                        name=restart_operation_name,
                        collection_name="replace_restart_noop",
                        depends_on=list(dag_node.depends_on),
                        noop=True,
                        host_names=None,
                    )
                    # Create and register the stop operation
                    stop_operation_name = dag_node.name.name.replace("_start", "_stop")
                    other_operations[stop_operation_name] = Operation(
                        name=stop_operation_name,
                        collection_name="replace_stop_noop",
                        depends_on=list(dag_node.depends_on),
                        noop=True,
                        host_names=None,
                    )

        # We can't merge the two for loops to handle the case where a playbook operation
        # is defined in a first collection but not used in the DAG and then used in
        # the DAG in a second collection.
        for collection in self._collection_readers:
            # Load playbook operations to complete the operations list with the
            # operations that are not defined in the DAG files
            for operation_name, playbook in self.playbooks.items():
                if operation_name in dag_operations:
                    continue
                if operation_name in other_operations:
                    logger.debug(
                        f"'{operation_name}' defined in "
                        f"'{other_operations[operation_name].collection_name}' "
                        f"is overridden by '{collection.name}'"
                    )
                other_operations[operation_name] = Operation(
                    name=operation_name,
                    host_names=playbook.hosts,  # TODO: do not store the hosts in the Operation object
                    collection_name=collection.name,
                )

        return dag_operations, other_operations

    def _init_schemas(self) -> dict[str, ServiceSchema]:
        schemas: dict[str, ServiceSchema] = {}
        for collection in self._collection_readers:
            for schema in collection.read_schemas():
                schemas.setdefault(schema.service, ServiceSchema()).add_schema(schema)
        return schemas

    def _init_entities(self) -> dict[str, set[ServiceComponentName]]:
        services_components: dict[str, set[ServiceComponentName]] = {}
        for operation in self.operations.values():
            service = services_components.setdefault(operation.name.service, set())
            if isinstance(operation.name, ServiceComponentName):
                service.add(operation.name)
        return services_components
