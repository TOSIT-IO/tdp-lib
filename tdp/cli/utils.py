# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click
from click.decorators import FC
from tabulate import tabulate

from tdp.core.collection import Collection
from tdp.core.collections import Collections
from tdp.core.variables.cluster_variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.models import DeploymentModel


def _collections_from_paths(
    ctx: click.Context, param: click.Parameter, value: list[Path]
) -> Collections:
    """Transforms a list of paths into a Collections object.

    Args:
        ctx: Click context.
        param: Click parameter.
        value: List of collections paths.

    Returns:
        Collections object from the paths.

    Raises:
        click.BadParameter: If the value is empty.
    """
    if not value:
        raise click.BadParameter("cannot be empty", ctx=ctx, param=param)

    collections_list = [Collection.from_path(path) for path in value]
    collections = Collections.from_collection_list(collections_list)

    return collections


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


def collections(func: FC) -> FC:
    return click.option(
        "--collection-path",
        "collections",
        envvar="TDP_COLLECTION_PATH",
        required=True,
        multiple=True,
        type=click.Path(resolve_path=True, path_type=Path),
        callback=_collections_from_paths,
        help="Path to the collection. Can be used multiple times.",
        is_eager=True,  # This option is used by other options, so we need to parse it first
    )(func)


def hosts(func: Optional[FC] = None, *, help: str) -> Callable[[FC], FC]:
    def decorator(fn: FC) -> FC:
        return click.option(
            "--host",
            "hosts",
            envvar="TDP_HOSTS",
            type=str,
            multiple=True,
            help=help,
        )(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)


def database_dsn(func: FC) -> FC:
    return click.option(
        "--database-dsn",
        envvar="TDP_DATABASE_DSN",
        required=True,
        type=str,
        help=(
            "Database Data Source Name, in sqlalchemy driver form "
            "example: sqlite:////data/tdp.db or sqlite+pysqlite:////data/tdp.db. "
            "You might need to install the relevant driver to your installation (such "
            "as psycopg2 for postgresql)."
        ),
    )(func)


def preview(func: FC) -> FC:
    return click.option(
        "--preview",
        is_flag=True,
        help="Preview the plan without running any action.",
    )(func)


def print_deployment(
    deployment: DeploymentModel, /, *, filter_out: Optional[list[str]] = None
) -> None:
    # Print general deployment infos
    click.secho("Deployment details", bold=True)
    click.echo(print_object(deployment.to_dict(filter_out=filter_out)))

    # Print deployment operations
    click.secho("\nOperations", bold=True)
    print_table(
        [o.to_dict(filter_out=filter_out) for o in deployment.operations],
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


def rolling_interval(func: FC) -> FC:
    return click.option(
        "-ri",
        "--rolling-interval",
        envvar="TDP_ROLLING_INTERVAL",
        type=int,
        help="Enable rolling restart with specific waiting time (in seconds) between component restart.",
    )(func)


def validate(func: FC) -> FC:
    return click.option(
        "--validate/--no-validate",
        # TODO: set default to True when schema validation is fully implemented
        # default=True,
        help="Should the command validate service variables against defined JSON schemas.",
    )(func)


def vars(func: Optional[FC] = None, *, exists=True) -> Callable[[FC], FC]:
    def decorator(fn: FC) -> FC:
        return click.option(
            "--vars",
            envvar="TDP_VARS",
            required=True,
            type=click.Path(resolve_path=True, path_type=Path, exists=exists),
            help="Path to the TDP variables.",
            is_eager=True,  # This option is used by other options, so we need to parse it first
        )(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)


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


class CatchGroup(click.Group):
    """Catch exceptions and print them to stderr."""

    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)

        except Exception as e:
            click.echo(f"Error: {e}", err=True)
