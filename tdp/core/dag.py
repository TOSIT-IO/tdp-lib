# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""
The `Dag` class reads YAML from collection's dag files
and validates it according to operations rules(cf. operations' rules section)
to build the DAG.

It is used to get a list of operations by performing a topological sort on the DAG
or on a subgraph of the DAG.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Callable, Generator, Iterable
from typing import TYPE_CHECKING, Optional, TypeVar

import networkx as nx

from tdp.core.constants import DEFAULT_SERVICE_PRIORITY, SERVICE_PRIORITY
from tdp.core.operation import Operation

if TYPE_CHECKING:
    from tdp.core.collections import Collections

T = TypeVar("T")

logger = logging.getLogger(__name__)


class IllegalNodeError(Exception):
    pass


class Dag:
    """Generate DAG with operations' dependencies."""

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

    def node_to_operation(
        self, node: str, restart: bool = False, stop: bool = False
    ) -> Operation:
        # ? Restart operations are now stored in collections.operations they can be
        # ? directly retrieved using the collections.get_operation method.
        # ? This method could be removed in the future.
        if restart and node.endswith("_start"):
            node = node.replace("_start", "_restart")
        elif stop and node.endswith("_start"):
            node = node.replace("_start", "_stop")
        return self.collections.get_operation(node)

    def topological_sort_key(
        self,
        items: Optional[Iterable[T]] = None,
        key: Optional[Callable[[T], str]] = None,
    ) -> Generator[T, None, None]:
        """Sorts the given iterable in topological order based on the DAG structure.

        The method supports custom mapping of the input items to the DAG nodes using a
        "key" function.

        By default, if no "key" function is provided, each item is used as is and must
        match against DAG nodes. If the "items" are not directly nodes of the DAG,
        the "key" function can be provided to map each item to its corresponding DAG
        node.

        The function ensures that multiple items mapped to the same DAG node maintain
        their relative order post sorting.

        Args:
            items: The iterable of items to sort. If None, sorts
              all DAG nodes.
            key: A function that maps an item to a DAG
              node. If None, items are converted to strings.

        Returns:
            A generator producing items in topologically sorted order.

        Example:
            dag = Dag(...)
            items = [("hdfs_start", "foo"), ("hdfs_config", "bar")]
            sorted_items = list(dag.topological_sort_key(items, key=lambda x: x[0]))
            # sorted_items = [("hdfs_config", "bar"), ("hdfs_start", "foo")]
        """
        # Map the items to the corresponding DAG nodes.
        # Use a dictionary to handle multiple items corresponding to a single DAG node.
        key_items = {}
        if items:
            for item in items:
                key_items.setdefault(item if key is None else key(item), []).append(
                    item
                )

        # If specific items are provided, restrict the graph to those nodes.
        # Otherwise, use the entire graph.
        graph = self.graph.subgraph(key_items.keys()) if key_items else self.graph

        # Define a priority function for nodes based on service priority.
        def priority_key(node: str) -> str:
            operation = self.operations[node]
            operation_priority = SERVICE_PRIORITY.get(
                operation.service_name, DEFAULT_SERVICE_PRIORITY
            )
            return f"{operation_priority:02d}_{node}"

        topo_sorted = nx.lexicographical_topological_sort(graph, priority_key)
        # Yield the sorted items. If custom items are provided, map the sorted nodes
        # back to the original items.
        if key_items:
            for node in topo_sorted:
                for item in key_items[node]:
                    yield item
        return topo_sorted

    def topological_sort(
        self,
        nodes: Optional[Iterable[str]] = None,
        restart: bool = False,
        stop: bool = False,
    ) -> list[Operation]:
        """Perform a topological sort on the DAG.

        Args:
            nodes: List of nodes to sort.
            restart: If True, restart operations are returned instead of start operations.

        Returns:
            List of operations sorted topologically.
        """
        return list(
            map(
                lambda node: self.node_to_operation(node, restart=restart, stop=stop),
                self.topological_sort_key(nodes),
            )
        )

    def get_operations(
        self,
        sources: Optional[Iterable[str]] = None,
        targets: Optional[Iterable[str]] = None,
        restart: bool = False,
        stop: bool = False,
    ) -> list[Operation]:
        if sources and targets:
            raise NotImplementedError("Cannot specify both sources and targets.")
        if sources:
            return self.get_operations_from_nodes(sources, restart=restart, stop=stop)
        elif targets:
            return self.get_operations_to_nodes(targets, restart=restart, stop=stop)
        return self.get_all_operations(restart=restart, stop=stop)

    def get_operations_to_nodes(
        self, nodes: Iterable[str], restart: bool = False, stop: bool = False
    ) -> list[Operation]:
        nodes_set = set(nodes)
        for node in nodes:
            if not self.graph.has_node(node):
                raise IllegalNodeError(f"{node} does not exists in the dag")
            nodes_set.update(nx.ancestors(self.graph, node))
        return self.topological_sort(nodes_set, restart=restart, stop=stop)

    def get_operations_from_nodes(
        self, nodes: Iterable[str], restart: bool = False, stop: bool = False
    ) -> list[Operation]:
        nodes_set = set(nodes)
        for node in nodes:
            if not self.graph.has_node(node):
                raise IllegalNodeError(f"{node} does not exists in the dag")
            nodes_set.update(nx.descendants(self.graph, node))
        return self.topological_sort(nodes_set, restart=restart, stop=stop)

    def get_all_operations(
        self, restart: bool = False, stop: bool = False
    ) -> list[Operation]:
        """gets all operations from the graph sorted topologically and lexicographically.

        :return: a topologically and lexicographically sorted string list
        :rtype: List[str]
        """
        return self.topological_sort(self.graph, restart=restart, stop=stop)

    def get_operation_descendants(
        self, nodes: list[str], restart: bool = False, stop: bool = False
    ) -> list[Operation]:
        """
        Retrieve all descendant operations for the specified nodes in the DAG.

        For each node in the provided list, this method identifies and returns
        all its descendant operations, excluding the input nodes themselves.

        Args:
            nodes: List of node names to find descendants for.
            restart: If True, restart the operation mapping process. Defaults to False.

        Returns:
            List of descendant operations.

        Raises:
            IllegalNodeError: Raised if a provided node does not exist in the DAG.

        Example:
            Given a DAG with nodes A -> B -> C and D -> E,
            get_operation_descendants(["A", "D"]) would return operations for B, C, and E.
        """
        nodes_set = set()
        for node in nodes:
            if not self.graph.has_node(node):
                raise IllegalNodeError(f"{node} does not exists in the dag")
            nodes_set.update(nx.descendants(self.graph, node))
        # Remove input nodes from the set to exclude them from the result.
        nodes_filtered = filter(lambda node: node not in nodes, nodes_set)
        return list(
            map(
                lambda node: self.node_to_operation(node, restart=restart, stop=stop),
                nodes_filtered,
            )
        )

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
