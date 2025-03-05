# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

import click
from tabulate import tabulate

from tdp.cli.params.collections_option import collections_option
from tdp.cli.params.hosts_option import hosts_option
from tdp.core.dag import Dag
from tdp.core.entities.operation import Operation, PlaybookOperation

if TYPE_CHECKING:
    from tdp.core.collections.collections import Collections


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
            if len(hosts) == 0
            or (
                not isinstance(operation, PlaybookOperation)
                or bool(set(operation.playbook.hosts) & set(hosts))
            )
        ]
        if topo_sort:
            sorted_operations = dag.topological_sort_key(
                operations, key=lambda operation: operation.name.name
            )
        else:
            sorted_operations = sorted(
                operations, key=lambda operation: operation.name.name
            )
        _print_operations(sorted_operations)
    else:
        operations = [
            operation
            for operation in collections.operations.values()
            if len(hosts) == 0
            or (
                not isinstance(operation, PlaybookOperation)
                or bool(set(operation.playbook.hosts) & set(hosts))
            )
        ]
        sorted_operations = sorted(
            operations, key=lambda operation: operation.name.name
        )
        _print_operations(sorted_operations)


def _print_operations(operations: Iterable[Operation], /):
    """Prints a list of operations."""
    click.echo(
        tabulate(
            [
                [
                    operation.name.name,
                    (
                        ", ".join(operation.playbook.hosts)
                        if isinstance(operation, PlaybookOperation)
                        else ""
                    ),
                ]
                for operation in operations
            ],
            headers=["Operation name", "Hosts"],
        )
    )
