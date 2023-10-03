# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import click
from click.decorators import FC
from tabulate import tabulate

from tdp.core.collection import Collection
from tdp.core.collections import Collections
from tdp.core.variables.cluster_variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.models import DeploymentLog


def _collections_from_paths(
    ctx: click.Context, param: click.Parameter, value: str
) -> Collections:
    """Transforms a list of paths into a Collections object.

    Args:
        ctx: Click context.
        param: Click parameter.
        value: List of collections path separated by os.pathsep.

    Returns:
        Collections object from the paths.

    Raises:
        click.BadParameter: If the value is empty.
    """
    if not value:
        raise click.BadParameter("cannot be empty", ctx=ctx, param=param)

    collections_list = [
        Collection.from_path(split) for split in value.split(os.pathsep)
    ]
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
        callback=_collections_from_paths,
        help=f"List of paths separated by your os' path separator ({os.pathsep}).",
    )(func)


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


def dry(func: FC) -> FC:
    return click.option(
        "--dry", is_flag=True, help="Execute dag without running any action."
    )(func)


def mock_deploy(func: FC) -> FC:
    return click.option(
        "--mock-deploy",
        envvar="TDP_MOCK_DEPLOY",
        is_flag=True,
        help="Mock the deploy, do not actually run the ansible playbook.",
    )(func)


def preview(func: FC) -> FC:
    return click.option(
        "--preview",
        is_flag=True,
        help="Preview the plan without running any action.",
    )(func)


def print_deployment(deployment: DeploymentLog) -> None:
    # Print general deployment infos
    click.secho("Deployment details", bold=True)
    click.echo(print_object(deployment.to_dict()))

    # Print related component version logs
    if deployment.component_version:
        click.secho("\nAffected components", bold=True)
        print_table(
            [
                c.to_dict()
                for c in deployment.component_version
                if c.component is not None
            ],
        )

    # Print deployment operations
    click.secho("\nOperations", bold=True)
    print_table(
        [o.to_dict() for o in deployment.operations],
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


def run_directory(func: FC) -> FC:
    return click.option(
        "--run-directory",
        envvar="TDP_RUN_DIRECTORY",
        type=Path,
        help="Working directory where the executor is launched (`ansible-playbook` for Ansible).",
        required=True,
    )(func)


def validate(func: FC) -> FC:
    return click.option(
        "--validate/--no-validate",
        default=True,
        help="Should the command validate service variables against defined JSON schemas.",
    )(func)


def vars(func=None, *, exists=True):
    def decorator(fn: FC) -> FC:
        return click.option(
            "--vars",
            envvar="TDP_VARS",
            required=True,
            type=click.Path(resolve_path=True, path_type=Path, exists=exists),
            help="Path to the tdp vars.",
        )(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)
