# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Iterable

import click
from tabulate import tabulate

from tdp.cli.utils import collections
from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.operation import Operation


@click.command(short_help="Display all available operations.")
@click.option(
    "--dag",
    "--dag-operations",
    "-D",
    "display_dag_operations",
    is_flag=True,
    help="Only shows operations from the DAG.",
)
@click.option(
    "--topo-sort",
    is_flag=True,
    help="Display DAG operations in topological order.",
)
@collections
def operations(collections: Collections, display_dag_operations: bool, topo_sort: bool):
    if topo_sort and not display_dag_operations:
        click.echo(
            "Warning: '--topo-sort' can only be used with '--dag' or '--dag-operations'."
        )

    if display_dag_operations:
        dag = Dag(collections)
        operations = dag.get_all_operations()
        if topo_sort:
            sorted_operations = dag.topological_sort_key(
                operations, key=lambda operation: operation.name
            )
        else:
            sorted_operations = sorted(operations, key=lambda operation: operation.name)
        _print_operations(sorted_operations)
    else:
        _print_operations(
            sorted(
                collections.operations.values(), key=lambda operation: operation.name
            )
        )


def _print_operations(operations: Iterable[Operation], /):
    """Prints a list of operations."""
    click.echo(
        tabulate(
            [[operation.name, operation.host_names or ""] for operation in operations],
            headers=["Operation name", "Hosts"],
        )
    )
