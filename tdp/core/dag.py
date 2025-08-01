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
from tdp.core.entities.entity_name import ServiceName
from tdp.core.entities.operation import (
    DagOperation,
    ForgedDagOperation,
    OperationName,
    OperationNoop,
    PlaybookOperation,
)

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.entities.operation import Operation

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
        self._operations = {
            operation.name: operation
            for operation in collections.operations.get_by_class(DagOperation)
        }
        validate_dag_nodes(self._operations, self._collections)
        self._graph = self._generate_graph(self.operations)

    @property
    def operations(self) -> dict[OperationName, DagOperation]:
        """DAG operations dictionary."""
        return self._operations

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

        # Define a priority function for nodes based on service priority
        def priority_key(node: str) -> str:
            operation = self.operations[OperationName.from_str(node)]
            operation_priority = SERVICE_PRIORITY.get(
                operation.name.service, DEFAULT_SERVICE_PRIORITY
            )
            return f"{operation_priority:02d}_{node}"

        topo_sorted = nx.lexicographical_topological_sort(self.graph, priority_key)

        # Yield the sorted items. If custom items are provided, map the sorted nodes
        # back to the original items.
        if key_items:
            for node in topo_sorted:
                if node in key_items:
                    yield from key_items[node]
        else:
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

    # TODO: can take a list of operations instead of a dict
    def _generate_graph(self, nodes: dict[OperationName, DagOperation]) -> nx.DiGraph:
        DG = nx.DiGraph()
        for operation_name, operation in nodes.items():
            if isinstance(operation, ForgedDagOperation):
                continue
            DG.add_node(str(operation_name))
            for dependency in operation.depends_on:
                if dependency not in nodes:
                    raise ValueError(
                        f'Dependency "{dependency}" does not exist for operation "{operation_name}"'
                    )
                DG.add_edge(str(dependency), str(operation_name))

        if nx.is_directed_acyclic_graph(DG):
            return DG
        else:
            raise ValueError("Not a DAG")


# TODO: remove Collections dependency
def validate_dag_nodes(
    nodes: dict[OperationName, DagOperation], collections: Collections
) -> None:
    r"""Validation rules :
    - \*_start operations can only be required from within its own service
    - \*_install operations should only depend on other \*_install operations
    - Each service (HDFS, HBase, Hive, etc) should have \*_install, \*_config, \*_init and \*_start operations even if they are "empty" (tagged with noop)
    - Operations tagged with the noop flag should not have a playbook defined in the collection
    - Each service action (config, start, init) except the first (install) must have an explicit dependency with the previous service operation within the same service
    """
    # key: service_name
    # value: set of available actions for the service
    # e.g. {'HDFS': {'install', 'config', 'init', 'start'}}
    services_actions = {}

    def warning(operation: DagOperation, message: str) -> None:
        if isinstance(operation, PlaybookOperation):
            collection_name = operation.playbook.collection_name
            logger.warning(message + f", collection: {collection_name}")
        else:
            logger.warning(message)

    for operation_name, operation in nodes.items():
        # No test are performed on forged operations
        if isinstance(operation, ForgedDagOperation):
            continue

        c_warning = functools.partial(warning, operation)
        for dependency in operation.depends_on:
            # *_start operations can only be required from within its own service
            dependency_service = nodes[dependency].name.service
            if (
                dependency.action == "start"
                and dependency_service != operation.name.service
            ):
                c_warning(
                    f"Operation '{operation_name}' is in service '{operation.name.service}', depends on "
                    f"'{dependency}' which is a start action in service '{dependency_service}' and should "
                    f"only depends on start action within its own service"
                )

            # *_install operations should only depend on other *_install operations
            if (
                operation.name.action == "install"
                and not dependency.action == "install"
            ):
                c_warning(
                    f"Operation '{operation_name}' is an install action, depends on '{dependency}' which is "
                    f"not an install action and should only depends on other install action"
                )

        # Each service (HDFS, HBase, Hive, etc) should have *_install, *_config, *_init and *_start actions
        # even if they are "empty" (tagged with noop)
        # Part 1
        service_actions = services_actions.setdefault(operation.name.service, set())
        if isinstance(operation.name.entity, ServiceName):
            service_actions.add(operation.name.action)

            # Each service action (config, start, init) except the first (install) must have an explicit
            # dependency with the previous service action within the same service
            actions_order = ["install", "config", "start", "init"]
            # Check only if the action is in actions_order and is not the first
            if (
                operation.name.action in actions_order
                and operation.name.action != actions_order[0]
            ):
                previous_action = actions_order[
                    actions_order.index(operation.name.action) - 1
                ]
                previous_service_action = OperationName(
                    operation.name.entity, previous_action
                )
                previous_service_action_found = False
                # Loop over dependency and check if the service previous action is found
                for dependency in operation.depends_on:
                    if dependency == previous_service_action:
                        previous_service_action_found = True
                if not previous_service_action_found:
                    c_warning(
                        f"Operation '{operation_name}' is a service action and has to depend on "
                        f"'{previous_service_action}'"
                    )

        # Operations tagged with the noop flag should not have a playbook defined in the collection

        #! This case can't happen because no operation inherits both PlaybookOperation and NoOp
        if str(operation_name) in collections.playbooks:
            if isinstance(operation, OperationNoop):
                c_warning(
                    f"Operation '{operation_name}' is noop and the playbook should not exist"
                )
        else:
            if not isinstance(operation, OperationNoop):
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
