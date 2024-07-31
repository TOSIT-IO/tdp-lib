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
from collections import OrderedDict
from collections.abc import Mapping, Sequence

from tdp.core.collection import Collection
from tdp.core.entities.hostable_entity_name import ServiceComponentName
from tdp.core.entities.operation import Operations
from tdp.core.operation import Operation
from tdp.core.variables.schema.service_schema import ServiceSchema

logger = logging.getLogger(__name__)


class Collections(Mapping[str, Collection]):
    """A mapping of collection name to Collection instance.

    This class also gather operations from all collections and filter them by their
    presence or not in the DAG.
    """

    def __init__(self, collections: Mapping[str, Collection]):
        self._collections = collections
        self._dag_operations, self._other_operations = self._init_operations(
            self._collections
        )
        self._schemas = self._init_schemas(self._collections)

    def __getitem__(self, key):
        return self._collections.__getitem__(key)

    def __iter__(self):
        return self._collections.__iter__()

    def __len__(self):
        return self._collections.__len__()

    def get_collections_hosts(self):
        host_operation_list = [
            self.operations.__getitem__(operation).host_names
            for operation in self.operations
        ]
        hosts_set = set()
        for operation_hosts in host_operation_list:
            hosts_set.update(operation_hosts)

        return {host for host in hosts_set if host != "localhost"}

    @staticmethod
    def from_collection_list(collections: Sequence[Collection]) -> Collections:
        """Factory method to build Collections from a sequence of Collection.

        Ordering of the sequence is what will determine the loading order of the operations.
        An operation can override a previous operation.

        Args:
            collections: Ordered Sequence of Collection object.

        Returns:
            A Collections object.

        Raises:
            ValueError: If a collection name is duplicated.
        """
        return Collections(
            OrderedDict((collection.name, collection) for collection in collections)
        )

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
    def schemas(self) -> dict[str, ServiceSchema]:
        """Mapping of service with their variable schemas."""
        return self._schemas

    def _init_operations(
        self, collections: Mapping[str, Collection]
    ) -> tuple[Operations, Operations]:
        dag_operations = Operations()
        other_operations = Operations()

        for collection in collections.values():
            # Load DAG operations from the dag files
            for dag_node in collection.dag_nodes:
                existing_operation = dag_operations.get(dag_node.name)

                # The read_operation is associated with a playbook defined in the
                # current collection
                if playbook := collection.playbooks.get(dag_node.name):
                    # TODO: would be nice to dissociate the Operation class from the playbook and store the playbook in the Operation
                    dag_operation_to_register = Operation(
                        name=dag_node.name,
                        collection_name=collection.name,
                        host_names=playbook.hosts,  # TODO: do not store the hosts in the Operation object
                        depends_on=dag_node.depends_on.copy(),
                    )
                    # If the operation is already registered, merge its dependencies
                    if existing_operation:
                        dag_operation_to_register.depends_on.extend(
                            dag_operations[dag_node.name].depends_on
                        )
                        # Print a warning if we override a playbook operation
                        if not existing_operation.noop:
                            logger.debug(
                                f"'{dag_node.name}' defined in "
                                f"'{existing_operation.collection_name}' "
                                f"is overridden by '{collection.name}'"
                            )
                    # Register the operation
                    dag_operations[dag_node.name] = dag_operation_to_register
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
                dag_operations[dag_node.name] = Operation(
                    name=dag_node.name,
                    collection_name=collection.name,
                    depends_on=dag_node.depends_on.copy(),
                    noop=True,
                    host_names=None,
                )
                # 'restart' and 'stop' operations are not defined in the DAG file
                # for noop, they need to be generated from the start operations
                if dag_node.name.endswith("_start"):
                    logger.debug(
                        f"'{dag_node.name}' is noop, creating the associated "
                        "restart and stop operations"
                    )
                    # Create and register the restart operation
                    restart_operation_name = dag_node.name.replace("_start", "_restart")
                    other_operations[restart_operation_name] = Operation(
                        name=restart_operation_name,
                        collection_name="replace_restart_noop",
                        depends_on=dag_node.depends_on.copy(),
                        noop=True,
                        host_names=None,
                    )
                    # Create and register the stop operation
                    stop_operation_name = dag_node.name.replace("_start", "_stop")
                    other_operations[stop_operation_name] = Operation(
                        name=stop_operation_name,
                        collection_name="replace_stop_noop",
                        depends_on=dag_node.depends_on.copy(),
                        noop=True,
                        host_names=None,
                    )

        # We can't merge the two for loops to handle the case where a playbook operation
        # is defined in a first collection but not used in the DAG and then used in
        # the DAG in a second collection.
        for collection in collections.values():
            # Load playbook operations to complete the operations list with the
            # operations that are not defined in the DAG files
            for operation_name, playbook in collection.playbooks.items():
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

    def _init_schemas(
        self, collections: Mapping[str, Collection]
    ) -> dict[str, ServiceSchema]:
        schemas: dict[str, ServiceSchema] = {}
        for collection in collections.values():
            for schema in collection.schemas:
                schemas.setdefault(schema.service, ServiceSchema()).add_schema(schema)
        return schemas

    def get_components_from_service(
        self, service_name: str
    ) -> set[ServiceComponentName]:
        """Retrieve the distinct components associated with a specific service.

        This method fetches and returns the unique component names tied to a given
        service. The input service is not returned.

        Args:
            service_name: The name of the service for which to retrieve associated
              components.

        Returns:
            A set containing the unique names of components related to the provided
              service.
        """
        return {
            ServiceComponentName(service_name, operation.component_name)
            for operation in self.operations.values()
            if operation.service_name == service_name
            and not operation.is_service_operation()
        }
