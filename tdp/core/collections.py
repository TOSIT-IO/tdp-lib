# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Mapping
from typing import TYPE_CHECKING, Iterable
from typing import Mapping as MappingType
from typing import Sequence

import yaml

from tdp.core.operation import Operation
from tdp.core.service_component_host_name import ServiceComponentHostName
from tdp.core.service_component_name import ServiceComponentName

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

if TYPE_CHECKING:
    from tdp.core.collection import Collection

logger = logging.getLogger("tdp").getChild("collections")


class MissingOperationError(Exception):
    pass


class MissingHostForOperationError(Exception):
    pass


class Collections(Mapping):
    """A mapping of collection name to Collection instance.

    This class also gather operations from all collections and filter them by their presence or not in the DAG.

    Attributes:
        collections: Mapping of collection name to Collection instance.
        dag_operations: Mapping of operation name that are in the DAG to Operation instance.
        other_operations: Mapping of operation name that are not in the DAG to Operation instance.
    """

    def __init__(self, collections: MappingType[str, Collection]):
        self._collections = collections
        self._dag_operations = None
        self._other_operations = None
        self._init_operations()

    def __getitem__(self, key):
        return self._collections.__getitem__(key)

    def __iter__(self):
        return self._collections.__iter__()

    def __len__(self):
        return self._collections.__len__()

    @staticmethod
    def from_collection_list(collections: Sequence[Collection]) -> "Collections":
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
        collections = OrderedDict(
            (collection.name, collection) for collection in collections
        )

        return Collections(collections)

    @property
    def collections(self) -> "Collections":
        """Mapping of collection name to Collection instance."""
        return self._collections

    @collections.setter
    def collections(self, collections: "Collections"):
        self._collections = collections
        self._init_operations()

    @property
    def dag_operations(self) -> MappingType[str, Operation]:
        """Mapping of operation name that are defined in dag files to their Operation instance."""
        return self._dag_operations

    @property
    def other_operations(self) -> MappingType[str, Operation]:
        """Mapping of operation name that aren't in dag files to their Operation instance."""
        return self._other_operations

    @property
    def operations(self) -> MappingType[str, Operation]:
        """Mapping of all operation name to Operation instance."""
        operations = {}
        if self._dag_operations:
            operations.update(self._dag_operations)
        if self._other_operations:
            operations.update(self._other_operations)
        return operations

    def _init_operations(self):
        self._dag_operations = {}
        self._other_operations = {}

        # Init DAG Operations
        for collection_name, collection in self._collections.items():
            for dag_yaml_file in collection.dag_yamls:
                operations_list = None
                with dag_yaml_file.open("r") as operations_file:
                    operations_list = yaml.load(operations_file, Loader=Loader) or []

                for operation in operations_list:
                    operation_name = operation["name"]
                    if operation_name in self._dag_operations:
                        logger.debug(
                            f"DAG Operation '{operation_name}' defined in collection "
                            f"'{self._dag_operations[operation_name].collection_name}' "
                            f"is merged with collection '{collection_name}'"
                        )
                        self._dag_operations[operation_name].depends_on.extend(
                            operation["depends_on"]
                        )
                    else:
                        host_names = None
                        if not operation.get("noop", False):
                            host_names = collection.get_hosts_from_playbook(
                                operation_name
                            )

                        self._dag_operations[operation_name] = Operation(
                            collection_name=collection_name,
                            host_names=host_names,
                            **operation,
                        )
                        # Restart operations are not defined in the DAG for noop,
                        # they are generated from the start operations.
                        if (
                            "noop" in operation
                            and operation["noop"]
                            and "_start" in operation_name
                        ):
                            restart_operation_name = operation_name.replace(
                                "_start", "_restart"
                            )
                            operation.update(
                                {
                                    "name": restart_operation_name,
                                }
                            )
                            logger.debug(
                                f"DAG Operation '{operation_name}' is noop, "
                                f"creating the associated restart operation."
                            )
                            self._other_operations[restart_operation_name] = Operation(
                                collection_name="replace_restart_noop",
                                **operation,
                            )

        # Init Operations not in the DAG
        for collection_name, collection in self._collections.items():
            for operation_name, _ in collection.playbooks.items():
                if operation_name in self._dag_operations:
                    continue
                if operation_name in self._other_operations:
                    logger.info(
                        f"Operation '{operation_name}' defined in collection "
                        f"'{self._other_operations[operation_name].collection_name}' "
                        f"is overridden by collection '{collection_name}'"
                    )

                self._other_operations[operation_name] = Operation(
                    name=operation_name,
                    host_names=collection.get_hosts_from_playbook(operation_name),
                    collection_name=collection_name,
                )

    def get_service_schema(self, service_name: str) -> dict:
        """Get the service's schema.

        Args:
            service_name: Name of the service.

        Returns:
            A dict with the JSON Schema of the service.
        """
        schemas = list(
            filter(
                bool,
                (
                    collection.get_service_schema(service_name)
                    for collection in self._collections.values()
                ),
            )
        )
        return dict(allOf=schemas) if schemas else {}

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
            ServiceComponentName(
                service_name=service_name, component_name=operation.component_name
            )
            for operation in self.operations.values()
            if operation.service_name == service_name
            and not operation.is_service_operation()
        }

    def get_components_hosts_from_service(
        self, service_name: str
    ) -> set[ServiceComponentHostName]:
        """Retrieve the distinct components with host associated with a specific
        service.

        This method fetches and returns the unique component names tied to a given
        service. The input service is not returned.

        Args:
            service_name: The name of the service for which to retrieve associated
              components.

        Returns:
            A set containing the unique names of components related to the provided
              service.
        """
        result = set()
        for operation in self.operations.values():
            if (
                operation.service_name != service_name
                or operation.is_service_operation()
            ):
                continue
            for host in operation.host_names:
                result.add(
                    ServiceComponentHostName(
                        ServiceComponentName(service_name, operation.component_name),
                        host,
                    )
                )
        return result

    def get_operation(self, operation_name: str) -> Operation:
        """Get an operation by its name.

        Args:
            operation_name: Name of the operation.

        Returns:
            The Operation instance.
        """
        if operation_name not in self.operations:
            raise MissingOperationError(
                f"Operation {operation_name} not found in collections."
            )
        return self.operations[operation_name]

    def check_operations_exist(self, operations_names: Iterable[str]) -> None:
        """Check that all operations exist.

        Args:
            operations_names: List of operation names to check.

        Raises:
            MissingOperationError: If an operation is missing.
        """
        for operation_name in operations_names:
            if operation_name not in self.operations:
                raise MissingOperationError(
                    f"Operation {operation_name} not found in collections."
                )

    def check_operations_hosts_exist(
        self, operation_names: Iterable[str], host_names: Iterable[str]
    ) -> None:
        """Check that all operations exist and hosts exist for all operations.

        Args:
            operation_names: Iterable of operation names to check.
            host_names: Iterable of host names to check for each operation.

        Raises:
            MissingHostForOperationError: If a host name is missing for an operation.
        """
        for operation_name in operation_names:
            operation = self.get_operation(operation_name)
            for host_name in host_names:
                if host_name not in operation.host_names:
                    raise MissingHostForOperationError(
                        f"Host {host_name} not found for operation {operation.name}. Valid hosts are {operation.host_names}"
                    )
