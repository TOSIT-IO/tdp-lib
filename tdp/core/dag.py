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

import logging
from collections.abc import Callable, Generator, Iterable
from typing import TYPE_CHECKING, Optional, TypeVar

import networkx as nx

from tdp.core.constants import DEFAULT_SERVICE_PRIORITY, SERVICE_PRIORITY
from tdp.core.entities.operation import Operations
from tdp.core.operation import Operation

if TYPE_CHECKING:
    from tdp.core.collections import Collections

T = TypeVar("T")

logger = logging.getLogger(__name__)


class IllegalNodeError(Exception):
    pass


class Dag:
    """Generate DAG with operations' dependencies."""

    # TODO: init with dag operations only
    def __init__(self, collections: Collections):
        """Initialize a DAG instance from a Collections.

        Args:
            collections: Collections instance.
        """
        self._collections = collections
        validate_dag_nodes(self._collections.dag_operations)
        self._graph = self._generate_graph(self._collections.dag_operations)

    @property
    def graph(self) -> nx.DiGraph:
        """DAG graph."""
        return self._graph

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
        return self._collections.operations[node]

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
            operation = self._collections.dag_operations[node]
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

    def _generate_graph(self, nodes: Operations) -> nx.DiGraph:
        DG = nx.DiGraph()
        for operation_name, operation in nodes.items():
            DG.add_node(operation_name)
            for dependency in operation.depends_on:
                # Check if the dependency exists
                if dependency not in nodes:
                    raise ValueError(
                        f"Operation '{operation.name}' depends on '{dependency}' which "
                        "doesn't exist."
                    )
                DG.add_edge(dependency, operation_name)

        if nx.is_directed_acyclic_graph(DG):
            return DG
        else:
            raise ValueError("Not a DAG")


def validate_dag_nodes(nodes: Operations) -> None:
    """Validate the DAG nodes.

    Validation rules:

    - Start operations can only be required from within its own service
    - Install operations should only depend on other install operations
    - Service operations should have the following dependency chain within the same
      service: install > config > start > init
    """
    actions_order = ["install", "config", "start", "init"]
    services_actions: dict[str, set[str]] = {}  # {service_name: {actions}}

    for operation in nodes.values():
        for dependency in operation.depends_on:
            # Start operations can only be required from within its own service
            dependency_service = nodes[dependency].service_name
            if (
                dependency.endswith("_start")
                and dependency_service != operation.service_name
            ):
                logger.warning(
                    f"Operation '{operation.name}' depends on '{dependency}' which "
                    "starts a diffent service. Start operations can only be required "
                    "from their own service."
                )

            # Install operations should only depend on other install operations
            if operation.name.endswith("_install") and not dependency.endswith(
                "_install"
            ):
                logger.warning(
                    f"Operation '{operation.name}' depends on '{dependency}' which is "
                    f"not an install action. Install operations should only depends on "
                    "other install operations."
                )

        # Service operations should respect the actions dependency chain
        if operation.is_service_operation():
            # Save the current action for the service for a later check
            services_actions.setdefault(operation.service_name, set()).add(
                operation.action_name
            )

            if (
                operation.action_name in actions_order
                and operation.action_name != actions_order[0]
            ):
                previous_action = actions_order[
                    actions_order.index(operation.action_name) - 1
                ]
                previous_service_action = f"{operation.service_name}_{previous_action}"
                if previous_service_action not in operation.depends_on:
                    logger.warning(
                        f"Operation '{operation.name}' doesn't depend on "
                        f"'{previous_service_action}'. Service operations should have "
                        "the following dependency chain within the same service: "
                        " > ".join(actions_order)
                    )

    # Service should at least define the 4 basic operations
    for service, actions in services_actions.items():
        if not actions.issuperset(set(actions_order)):
            logger.warning(
                f"Service '{service}' is missing {set(actions_order) - actions}. "
                f"Service should have the following actions specified: {actions_order}."
            )
