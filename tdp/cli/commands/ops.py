# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Iterable

import click
from tabulate import tabulate

from tdp.cli.params import collections_option, hosts_option
from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.operation import Operation


@click.command()
@click.option(
    "--dag",
    "--dag-operations",
    "-D",
    "display_dag_operations",
    is_flag=True,
    help="Only shows operations from the DAG.",
)
@hosts_option(help="Hosts where operations are launched. Can be used multiple times.")
@click.option(
    "--topo-sort",
    is_flag=True,
    help="Display DAG operations in topological order.",
)
@collections_option
def ops(
    collections: Collections,
    display_dag_operations: bool,
    hosts: tuple[str],
    topo_sort: bool,
):
    """Display all available operations."""
    if topo_sort and not display_dag_operations:
        click.echo(
            "Warning: `--topo-sort` can only be used with `--dag` or `--dag-operations`."
        )

    if display_dag_operations:
        dag = Dag(collections)
        operations = [
            operation
            for operation in dag.get_all_operations()
            if len(hosts) == 0 or bool(set(operation.host_names) & set(hosts))
        ]
        if topo_sort:
            sorted_operations = dag.topological_sort_key(
                operations, key=lambda operation: operation.str_name
            )
        else:
            sorted_operations = sorted(
                operations, key=lambda operation: operation.str_name
            )
        _print_operations(sorted_operations)
    else:
        operations = [
            operation
            for operation in collections.operations.values()
            if len(hosts) == 0 or bool(set(operation.host_names) & set(hosts))
        ]
        sorted_operations = sorted(operations, key=lambda operation: operation.str_name)
        _print_operations(sorted_operations)


def _print_operations(operations: Iterable[Operation], /):
    """Prints a list of operations."""
    click.echo(
        tabulate(
            [
                [operation.str_name, operation.host_names or ""]
                for operation in operations
            ],
            headers=["Operation name", "Hosts"],
        )
    )
