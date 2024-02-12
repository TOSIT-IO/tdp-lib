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
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError

from tdp.core.collection import Collection
from tdp.core.operation import Operation
from tdp.core.service_component_host_name import ServiceComponentHostName
from tdp.core.service_component_name import ServiceComponentName

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

logger = logging.getLogger(__name__)


class TDPLibDagOperationModel(BaseModel):
    """Model for a TDP operation defined in a tdp_lib_dag file."""

    model_config = ConfigDict(extra="ignore")

    name: str
    depends_on: list[str] = []
    noop: bool = False


class TDPLibDagModel(BaseModel):
    """Model for a TDP DAG defined in a tdp_lib_dag file."""

    model_config = ConfigDict(extra="ignore")

    operations: list[TDPLibDagOperationModel]


def read_tdp_lib_dag_file(tdp_lib_dag_file_path: Path) -> list[TDPLibDagOperationModel]:
    """Read a tdp_lib_dag file and return a list of DAG operations."""
    with tdp_lib_dag_file_path.open("r") as operations_file:
        file_content = yaml.load(operations_file, Loader=Loader)

    try:
        tdp_lib_dag = TDPLibDagModel(operations=file_content)
        return tdp_lib_dag.operations
    except ValidationError as e:
        logger.error(
            f"Error while parsing tdp_lib_dag file {tdp_lib_dag_file_path}: {e}"
        )
        raise


class MissingOperationError(Exception):
    pass


class MissingHostForOperationError(Exception):
    pass


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
        return Collections(
            OrderedDict((collection.name, collection) for collection in collections)
        )

    @property
    def dag_operations(self) -> dict[str, Operation]:
        """Mapping of operation name that are defined in dag files to their Operation instance."""
        return self._dag_operations

    @property
    def other_operations(self) -> dict[str, Operation]:
        """Mapping of operation name that aren't in dag files to their Operation instance."""
        return self._other_operations

    @property
    def operations(self) -> dict[str, Operation]:
        """Mapping of all operation name to Operation instance."""
        operations = {}
        if self._dag_operations:
            operations.update(self._dag_operations)
        if self._other_operations:
            operations.update(self._other_operations)
        return operations

    def _init_operations(
        self, collections: Mapping[str, Collection]
    ) -> tuple[dict[str, Operation], dict[str, Operation]]:
        dag_operations: dict[str, Operation] = {}
        other_operations: dict[str, Operation] = {}

        # Init DAG Operations
        for collection_name, collection in collections.items():
            for dag_file in collection.dag_yamls:
                for read_operation in read_tdp_lib_dag_file(dag_file):
                    if read_operation.name in dag_operations:
                        # Merge the operation with the existing one
                        logger.debug(
                            f"DAG Operation '{read_operation.name}' defined in collection "
                            f"'{dag_operations[read_operation.name].collection_name}' "
                            f"is merged with collection '{collection_name}'"
                        )
                        dag_operations[read_operation.name].depends_on.extend(
                            read_operation.depends_on
                        )
                        continue

                    # Create the operation
                    dag_operations[read_operation.name] = Operation(
                        name=read_operation.name,
                        collection_name=collection_name,  # TODO: this is the collection that defines the DAG where the operation is defined, not the collection that defines the operation
                        depends_on=read_operation.depends_on,
                        noop=read_operation.noop,
                        host_names=(
                            None
                            if read_operation.noop
                            else collection.get_hosts_from_playbook(read_operation.name)
                        ),
                    )
                    # 'restart' and 'stop' operations are not defined in the DAG for
                    # noop, they need to be generated from the start operations.
                    if read_operation.noop and read_operation.name.endswith("_start"):
                        logger.debug(
                            f"DAG Operation '{read_operation.name}' is noop, "
                            f"creating the associated restart and stop operations."
                        )
                        # Create and store the restart operation
                        restart_operation_name = read_operation.name.replace(
                            "_start", "_restart"
                        )
                        other_operations[restart_operation_name] = Operation(
                            name=restart_operation_name,
                            collection_name="replace_restart_noop",
                            depends_on=read_operation.depends_on,
                            noop=True,
                            host_names=None,
                        )
                        # Create and store the stop operation
                        stop_operation_name = read_operation.name.replace(
                            "_start", "_stop"
                        )
                        other_operations[stop_operation_name] = Operation(
                            name=stop_operation_name,
                            collection_name="replace_stop_noop",
                            depends_on=read_operation.depends_on,
                            noop=True,
                            host_names=None,
                        )

        # Init Operations not in the DAG
        for collection_name, collection in collections.items():
            for operation_name, _ in collection.playbooks.items():
                if operation_name in dag_operations:
                    continue
                if operation_name in other_operations:
                    logger.info(
                        f"Operation '{operation_name}' defined in collection "
                        f"'{other_operations[operation_name].collection_name}' "
                        f"is overridden by collection '{collection_name}'"
                    )

                other_operations[operation_name] = Operation(
                    name=operation_name,
                    host_names=collection.get_hosts_from_playbook(operation_name),
                    collection_name=collection_name,
                )

        return dag_operations, other_operations

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
        self.check_operation_exists(operation_name)
        return self.operations[operation_name]

    def check_operation_exists(self, operation_name: str) -> None:
        """Check that an operation exists.

        Args:
            operation_name: Name of the operation.

        Raises:
            MissingOperationError: If the operation is missing.
        """
        if operation_name not in self.operations:
            raise MissingOperationError(
                f"Operation {operation_name} not found in collections."
            )

    # TODO: remove and use check_operation_exists in a loop
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

    def get_operation_or_none(
        self, service_component_name: ServiceComponentName, action_name: str
    ) -> Optional[Operation]:
        """Get an operation by its name.

        Args:
            service_component_name: Name of the service.
            action_name: Name of the action.

        Returns:
            The Operation instance.
        """
        operation_name = f"{service_component_name.full_name}_{action_name}"
        try:
            return self.get_operation(operation_name)
        except MissingOperationError:
            return None
