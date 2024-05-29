# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import TYPE_CHECKING, Optional

import click
from tabulate import tabulate

from tdp.core.entities.deployment_entity import Deployment_entity
from tdp.core.entities.hosted_entity_status import HostedEntityStatus

if TYPE_CHECKING:
    from tdp.core.variables.cluster_variables import ClusterVariables


def check_services_cleanliness(cluster_variables: ClusterVariables) -> None:
    """Check that all services are in a clean state.

    Args:
        cluster_variables: Instance of ClusterVariables.

    Raises:
        click.ClickException: If some services are in a dirty state.
    """
    unclean_services = [
        service_variables.name
        for service_variables in cluster_variables.values()
        if not service_variables.clean
    ]
    if unclean_services:
        for name in unclean_services:
            click.echo(
                f'"{name}" repository is not in a clean state.'
                " Check that all modifications are committed."
            )
        raise click.ClickException(
            "Some services are in a dirty state, commit your modifications."
        )


def print_deployment(
    deployment: Deployment_entity, /, *, filter_out: Optional[list[str]] = None
) -> None:
    # Print general deployment infos
    click.secho("Deployment details", bold=True)
    click.echo(
        print_object(
            deployment.transform_to_deployment_model().to_dict(filter_out=filter_out)
        )
    )

    # Print deployment operations
    click.secho("\nOperations", bold=True)
    print_table(
        [
            o.transform_to_operation_model().to_dict(filter_out=filter_out)
            for o in deployment.operations
        ],
    )


def print_object(obj: dict) -> None:
    """Print an object in a human readable format.

    Args:
        obj: Object to print.
    """
    click.echo(
        tabulate(
            obj.items(),
            tablefmt="plain",
        )
    )


def print_table(rows) -> None:
    """Print a list of rows in a human readable format.

    Args:
        rows: List of rows to print.
    """
    click.echo(
        tabulate(
            rows,
            headers="keys",
        )
    )


def print_hosted_entity_status_log(sch_status: Iterable[HostedEntityStatus]) -> None:
    print_table([status.export_tabulate() for status in sch_status])


def _parse_line(line: str) -> tuple[str, Optional[str], Optional[list[str]]]:
    """Parses a line which contains an operation, and eventually a host and extra vars.

    Args:
        line: Line to be parsed.

    Returns:
        Operation, host and extra vars.
    """
    parsed_line = re.match(
        r"^(.*?)( on .*?){0,1}( ?with .*?){0,1}( ?on .*?){0,1}$", line
    )

    if parsed_line is None:
        raise ValueError(
            "Error on line '"
            + line
            + "': it must be 'OPERATION [on HOST] [with EXTRA_VARS[,EXTRA_VARS]].'"
        )

    if parsed_line.group(1).split(" ")[0] == "":
        raise ValueError("Error on line '" + line + "': it must contain an operation.")

    if len(parsed_line.group(1).strip().split(" ")) > 1:
        raise ValueError("Error on line '" + line + "': only 1 operation is allowed.")

    if parsed_line.group(2) is not None and parsed_line.group(4) is not None:
        raise ValueError(
            "Error on line '" + line + "': only 1 host is allowed in a line."
        )

    operation = parsed_line.group(1).strip()

    # Get the host and test if it is declared
    if parsed_line.group(2) is not None:
        host = parsed_line.group(2).split(" on ")[1]
        if host == "":
            raise ValueError(
                "Error on line '" + line + "': host is required after 'on' keyword."
            )
    elif parsed_line.group(4) is not None:
        host = parsed_line.group(4).split(" on ")[1]
        if host == "":
            raise ValueError(
                "Error on line '" + line + "': host is required after 'on' keyword."
            )
    else:
        host = None

    # Get the extra vars and test if they are declared
    if parsed_line.group(3) is not None:
        extra_vars = parsed_line.group(3).split(" with ")[1]
        if extra_vars == "":
            raise ValueError("Extra vars are required after 'with' keyword.")
        extra_vars = extra_vars.split(",")
        extra_vars = [item.strip() for item in extra_vars]
    else:
        extra_vars = None

    return (operation, host, extra_vars)


def parse_file(file_name) -> list[tuple[str, Optional[str], Optional[list[str]]]]:
    """Parses a file which contains operations, hosts and extra vars."""
    file_content = file_name.read()
    return [
        _parse_line(line)
        for line in file_content.split("\n")
        if line and not line.startswith("#")
    ]
