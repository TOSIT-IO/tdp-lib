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
from tdp.core.entities.operation import Operations, Playbook
from tdp.core.operation import LegacyOperation
from tdp.core.variables.schema.service_schema import ServiceSchema

logger = logging.getLogger(__name__)


class Collections(Mapping[str, Collection]):
    """A mapping of collection name to Collection instance.

    This class also gather operations from all collections and filter them by their
    presence or not in the DAG.
    """

    def __init__(self, collections: Mapping[str, Collection]):
        self._collections = collections
        self._playbooks = self._init_playbooks(self._collections)
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
    def playbooks(self) -> dict[str, Playbook]:
        """Available playbooks.

        Playbooks that are defined in multiple collections are overridden by the last
        collection in the list."""
        return self._playbooks

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

    def _init_playbooks(
        self, collections: Mapping[str, Collection]
    ) -> dict[str, Playbook]:
        playbooks: dict[str, Playbook] = {}
        for collection in collections.values():
            for operation_name, playbook in collection.playbooks.items():
                if operation_name in playbooks:
                    logger.warning(
                        f"Playbook '{operation_name}' defined in "
                        f"'{playbook.collection_name}' is overridden by "
                        f"'{collection.name}'"
                    )
                playbooks[operation_name] = playbook
        return playbooks

    def _init_operations(
        self, collections: Mapping[str, Collection]
    ) -> tuple[Operations, Operations]:
        dag_operations = Operations()
        other_operations = Operations()

        for collection in collections.values():
            # Load DAG operations from the dag files
            for node in collection.dag_nodes:
                existing_operation = dag_operations.get(node.name)

                # The DAG node is associated with a playbook defined in the current
                # collection
                if playbook := collection.playbooks.get(node.name):
                    # If a the action is a start, check if the associated restart and
                    # stop playbooks are defined
                    if node.name.endswith("_start"):
                        if (
                            restart_operation_name := node.name.replace(
                                "_start", "_restart"
                            )
                        ) not in collection.playbooks:
                            logger.warning(
                                f"Missing {restart_operation_name} playbook in "
                                f"{collection.name}. Each start playbook should have "
                                "an associated restart playbook."
                            )
                        if (
                            stop_operation_name := node.name.replace("_start", "_stop")
                        ) not in collection.playbooks:
                            logger.warning(
                                f"Missing {stop_operation_name} playbook in "
                                f"{collection.name}. Each stop playbook should have an "
                                "associated restart playbook."
                            )

                    # TODO: would be nice to dissociate the Operation class from the playbook and store the playbook in the Operation
                    operation_to_register = LegacyOperation(
                        name=node.name,
                        collection_name=collection.name,
                        host_names=playbook.hosts,  # TODO: do not store the hosts in the Operation object
                        depends_on=node.depends_on.copy(),
                    )
                    # If the operation is already registered, merge its dependencies
                    if existing_operation:
                        logger.debug(
                            f"'{existing_operation.name}' dependencies are extended by "
                            f"'{collection.name}'"
                        )
                        operation_to_register.depends_on.extend(
                            dag_operations[node.name].depends_on
                        )
                        # Print a warning if we override a playbook operation
                        if not existing_operation.noop:
                            logger.debug(
                                f"'{existing_operation.name}' defined in "
                                f"'{existing_operation.collection_name}' "
                                f"is overridden by '{collection.name}'."
                            )
                    # Register the operation
                    dag_operations[node.name] = operation_to_register
                    continue

                # The read_operation is already registered
                if existing_operation:
                    logger.debug(
                        f"'{existing_operation.name}' dependencies are extended by "
                        f"'{collection.name}'"
                    )
                    existing_operation.depends_on.extend(node.depends_on)
                    continue

                # From this point, the read_operation is a noop as it is not defined
                # in the current nor the previous collections

                # Create and register the operation
                dag_operations[node.name] = LegacyOperation(
                    name=node.name,
                    collection_name=collection.name,
                    depends_on=node.depends_on.copy(),
                    noop=True,
                    host_names=None,
                )
                # 'restart' and 'stop' operations are not defined in the DAG file
                # for noop, they need to be generated from the start operations
                if node.name.endswith("_start"):
                    logger.debug(
                        f"'{node.name}' is noop, creating the associated "
                        "restart and stop operations"
                    )
                    for action_name in ["_restart", "_stop"]:
                        operation_name = node.name.replace("_start", action_name)
                        other_operations[operation_name] = LegacyOperation(
                            name=operation_name,
                            collection_name=collection.name,
                            depends_on=node.depends_on.copy(),
                            noop=True,
                            host_names=None,
                        )

        # Register the operations that are not defined in the DAG files
        for operation_name, playbook in self.playbooks.items():
            if operation_name in dag_operations:
                continue
            other_operations[operation_name] = LegacyOperation(
                name=operation_name,
                host_names=playbook.hosts,  # TODO: do not store the hosts in the Operation object
                collection_name=playbook.collection_name,
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
