# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

# TEst

"""
The `Dag` class reads YAML from collection's dag files
and validates it according to operations rules(cf. operations' rules section)
to build the DAG.

It is used to get a list of operations by performing a topological sort on the DAG
or on a subgraph of the DAG.
"""

from __future__ import annotations

import fnmatch
import functools
import logging
import re
from typing import Optional, TYPE_CHECKING

import networkx as nx

from tdp.core.operation import Operation

if TYPE_CHECKING:
    from tdp.core.collections import Collections

logger = logging.getLogger("tdp").getChild("dag")

SERVICE_PRIORITY = {
    "exporter": 1,
    "zookeeper": 2,
    "hadoop": 3,
    "ranger": 4,
    "hdfs": 5,
    "yarn": 6,
    "hive": 7,
    "hbase": 8,
    "spark": 9,
    "spark3": 10,
    "knox": 11,
}
DEFAULT_SERVICE_PRIORITY = 99


class IllegalNodeError(Exception):
    pass


class Dag:
    """Generate DAG with operations' dependencies"""

    def __init__(self, collections: Collections):
        """Initialize a DAG instance from a Collections.

        Args:
            collections: Collections instance.
        """
        self._collections = collections
        self._operations = None
        self._graph = None
        self._yaml_files = None
        self._services = None
        self._services_operations = None

    @property
    def collections(self) -> Collections:
        """Collections instance."""
        return self._collections

    @collections.setter
    def collections(self, collections: Collections) -> None:
        self._collections = collections
        del self.operations

    @property
    def operations(self) -> dict[str, Operation]:
        """DAG operations dictionary."""
        if self._operations is not None:
            return self._operations

        self._operations = self._collections.dag_operations
        self.validate()
        return self._operations

    @operations.setter
    def operations(self, value: dict[str, Operation]) -> None:
        """Set operations and reset graph, services_operations and services."""
        self._operations = value
        del self.graph
        del self.services_operations
        del self.services

    @operations.deleter
    def operations(self) -> None:
        self.operations = None

    @property
    def services_operations(self) -> dict[str, list[Operation]]:
        """DAG operations dictionary grouped by service."""
        if self._services_operations is None:
            self._services_operations = {}
            for operation in self.operations.values():
                self._services_operations.setdefault(operation.service_name, []).append(
                    operation
                )
        return self._services_operations

    @services_operations.deleter
    def services_operations(self) -> None:
        self._services_operations = None
        del self.services

    @property
    def services(self) -> list[str]:
        """List of services in the DAG."""
        if self._services is None:
            self._services = list(self.services_operations.keys())
        return self._services

    @services.deleter
    def services(self) -> None:
        self._services = None

    @property
    def graph(self) -> nx.DiGraph:
        """DAG graph."""
        if self._graph is not None:
            return self._graph

        DG = nx.DiGraph()
        for operation_name, operation in self.operations.items():
            DG.add_node(operation_name)
            for dependency in operation.depends_on:
                if dependency not in self.operations:
                    raise ValueError(
                        f'Dependency "{dependency}" does not exist for operation "{operation_name}"'
                    )
                DG.add_edge(dependency, operation_name)

        if nx.is_directed_acyclic_graph(DG):
            self._graph = DG
            return self._graph
        else:
            raise ValueError("Not a DAG")

    @graph.setter
    def graph(self, value: nx.DiGraph) -> None:
        self._graph = value

    @graph.deleter
    def graph(self) -> None:
        self.graph = None

    def topological_sort(
        self, nodes: list[str] = None, restart: bool = False
    ) -> list[Operation]:
        """Perform a topological sort on the DAG.

        Args:
            nodes: List of nodes to sort.
            restart: If True, restart operations are returned instead of start operations.

        Returns:
            List of operations sorted topologically.
        """
        graph = self.graph
        if nodes:
            graph = self.graph.subgraph(nodes)

        def custom_key(node: str) -> str:
            operation = self.operations[node]
            operation_priority = SERVICE_PRIORITY.get(
                operation.service_name, DEFAULT_SERVICE_PRIORITY
            )
            return f"{operation_priority:02d}_{node}"

        def to_operation(node: str) -> Operation:
            operation = self.collections.get_operation(node)
            if restart:
                if node.endswith("_start"):
                    node = node.replace("_start", "_restart")
                    if operation.noop:
                        # if start operation is a noop, outputs a noop restart operation
                        return Operation(
                            name=node,
                            collection_name="replace_restart_noop",
                            noop=True,
                            depends_on=operation.depends_on,
                        )
                return self.collections.get_operation(node)
            return operation

        return list(
            map(
                lambda node: to_operation(node),
                nx.lexicographical_topological_sort(graph, custom_key),
            )
        )

    def get_operations(
        self,
        sources: Optional[list[str]] = None,
        targets: Optional[list[str]] = None,
        restart: bool = False,
    ) -> list[Operation]:
        if sources:
            return self.get_operations_from_nodes(sources, restart)
        elif targets:
            return self.get_operations_to_nodes(targets, restart)
        return self.get_all_operations(restart)

    def get_operations_to_nodes(
        self, nodes: list[str], restart: bool = False
    ) -> list[Operation]:
        nodes_set = set(nodes)
        for node in nodes:
            if not self.graph.has_node(node):
                raise IllegalNodeError(f"{node} does not exists in the dag")
            nodes_set.update(nx.ancestors(self.graph, node))
        return self.topological_sort(nodes_set, restart)

    def get_operations_from_nodes(
        self, nodes: list[str], restart: bool = False
    ) -> list[Operation]:
        nodes_set = set(nodes)
        for node in nodes:
            if not self.graph.has_node(node):
                raise IllegalNodeError(f"{node} does not exists in the dag")
            nodes_set.update(nx.descendants(self.graph, node))
        return self.topological_sort(nodes_set, restart)

    def get_all_operations(self, restart: bool = False) -> list[Operation]:
        """gets all operations from the graph sorted topologically and lexicographically.

        :return: a topologically and lexicographically sorted string list
        :rtype: List[str]
        """
        return self.topological_sort(self.graph, restart)

    def filter_operations_glob(
        self, operations: list[Operation], glob: str
    ) -> list[Operation]:
        return list(filter(lambda o: fnmatch.fnmatch(o.name, glob), operations))  # type: ignore

    def filter_operations_regex(
        self, operations: list[Operation], regex: str
    ) -> list[Operation]:
        compiled_regex = re.compile(regex)
        return list(filter(lambda o: compiled_regex.match(o.name), operations))  # type: ignore

    def validate(self) -> None:
        r"""Validation rules :
        - \*_start operations can only be required from within its own service
        - \*_install operations should only depend on other \*_install operations
        - Each service (HDFS, HBase, Hive, etc) should have \*_install, \*_config, \*_init and \*_start operations even if they are "empty" (tagged with noop)
        - Operations tagged with the noop flag should not have a playbook defined in the collection
        - Each service action (config, start, init) except the first (install) must have an explicit dependency with the previous service operation within the same service
        """
        # key: service_name
        # value: set of available actions for the service
        services_actions = {}

        def warning(collection_name: str, message: str) -> None:
            logger.warning(message + f", collection: {collection_name}")

        for operation_name, operation in self.operations.items():
            c_warning = functools.partial(warning, operation.collection_name)
            for dependency in operation.depends_on:
                # *_start operations can only be required from within its own service
                dependency_service = self.operations[dependency].service_name
                if (
                    dependency.endswith("_start")
                    and dependency_service != operation.service_name
                ):
                    c_warning(
                        f"Operation '{operation_name}' is in service '{operation.service_name}', depends on "
                        f"'{dependency}' which is a start action in service '{dependency_service}' and should "
                        f"only depends on start action within its own service"
                    )

                # *_install operations should only depend on other *_install operations
                if operation_name.endswith("_install") and not dependency.endswith(
                    "_install"
                ):
                    c_warning(
                        f"Operation '{operation_name}' is an install action, depends on '{dependency}' which is "
                        f"not an install action and should only depends on other install action"
                    )

            # Each service (HDFS, HBase, Hive, etc) should have *_install, *_config, *_init and *_start actions
            # even if they are "empty" (tagged with noop)
            # Part 1
            service_actions = services_actions.setdefault(operation.service_name, set())
            if operation.is_service_operation():
                service_actions.add(operation.action_name)

                # Each service action (config, start, init) except the first (install) must have an explicit
                # dependency with the previous service action within the same service
                actions_order = ["install", "config", "start", "init"]
                # Check only if the action is in actions_order and is not the first
                if (
                    operation.action_name in actions_order
                    and operation.action_name != actions_order[0]
                ):
                    previous_action = actions_order[
                        actions_order.index(operation.action_name) - 1
                    ]
                    previous_service_action = (
                        f"{operation.service_name}_{previous_action}"
                    )
                    previous_service_action_found = False
                    # Loop over dependency and check if the service previous action is found
                    for dependency in operation.depends_on:
                        if dependency == previous_service_action:
                            previous_service_action_found = True
                    if not previous_service_action_found:
                        c_warning(
                            f"Operation '{operation_name}' is a service action and has to depend on "
                            f"'{operation.service_name}_{previous_action}'"
                        )

            # Operations tagged with the noop flag should not have a playbook defined in the collection

            if operation_name in self._collections[operation.collection_name].playbooks:
                if operation.noop:
                    c_warning(
                        f"Operation '{operation_name}' is noop and the playbook should not exist"
                    )
            else:
                if not operation.noop:
                    c_warning(f"Operation '{operation_name}' should have a playbook")

        # Each service (HDFS, HBase, Hive, etc) should have *_install, *_config, *_init and *_start actions
        # even if they are "empty" (tagged with noop)
        # Part 2
        actions_for_service = {"install", "config", "start", "init"}
        for service, actions in services_actions.items():
            if not actions.issuperset(actions_for_service):
                logger.warning(
                    f"Service '{service}' have these actions {actions} and at least one action is missing from "
                    f"{actions_for_service}"
                )
