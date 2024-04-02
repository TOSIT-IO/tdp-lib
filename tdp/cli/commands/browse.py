# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Iterable
from typing import Optional

import click
from sqlalchemy import Engine

from tdp.cli.params import database_dsn_option
from tdp.cli.queries import (
    get_deployment,
    get_deployments,
    get_operation_records,
    get_planned_deployment,
)
from tdp.cli.utils import (
    print_deployment,
    print_object,
    print_table,
)
from tdp.core.models import DeploymentModel, OperationModel
from tdp.dao import Dao


@click.command()
@click.argument("deployment_id", required=False)
@click.argument("operation", required=False)
@click.option(
    "-p",
    "--plan",
    is_flag=True,
    help="Print the planned deployment, if exist.",
)
@click.option(
    "-l",
    "--last",
    is_flag=True,
    help="Print the last deployment.",
)
@click.option(
    "--limit",
    envvar="TDP_LIMIT",
    type=int,
    default=15,
    help="Limit number of deployments returned.",
)
@click.option(
    "--offset",
    envvar="TDP_OFFSET",
    type=int,
    default=0,
    help="At which offset the database query should start.",
)
@database_dsn_option
def browse(
    plan: bool,
    last: bool,
    limit: int,
    offset: int,
    db_engine: Engine,
    deployment_id: Optional[int] = None,
    operation: Optional[str] = None,
):
    """Browse deployments."""
    with Dao(db_engine) as dao:
        # Print last deployment plan
        if plan:
            deployment_plan = get_planned_deployment(dao.session)
            if deployment_plan:
                _print_deployment(deployment_plan)
            else:
                click.echo("No planned deployment found")
                click.echo("Create a deployment plan using the `tdp plan` command")
            return
        elif last:
            deployment = get_deployments(dao.session, limit=1, offset=0)
            if deployment:
                _print_deployment(deployment[0])
            else:
                click.echo("No deployment found")
                click.echo("Create a deployment plan using the `tdp plan` command")
            return

        # Print a specific operation
        if deployment_id and operation:
            _print_operations(
                get_operation_records(dao.session, deployment_id, operation)
            )
            return

        # Print a specific deployment
        if deployment_id:
            _print_deployment(get_deployment(dao.session, deployment_id))
            return

        # Print all deployments
        _print_deployments(get_deployments(dao.session, limit, offset))


def _print_deployments(deployments: Iterable[DeploymentModel]) -> None:
    """Print a list of deployments in a human readable format.

    Args:
        deployments: List of deployments to print.
    """
    if not deployments:
        click.echo("No deployment found")
        click.echo("Create a deployment plan using the `tdp plan` command")

    print_table(
        [d.to_dict(filter_out=["options"]) for d in deployments],
    )


def _print_deployment(deployment: DeploymentModel) -> None:
    """Print a deployment in a human readable format.

    Args:
        deployment: Deployment to print.
    """
    if not deployment:
        click.echo("Deployment does not exist.")
        return

    print_deployment(deployment, filter_out=["logs"])


def _print_operations(operations: list[OperationModel]) -> None:
    """Print a list of operations in a human readable format.

    Args:
        operation: Operation to print.
    """
    # Print general operation infos
    click.secho("Operation(s) details", bold=True)
    for operation in operations:
        click.echo(print_object(operation.to_dict()))
        # Print operation records
        if operation.logs:
            click.secho("\nOperations", bold=True)
            click.echo(str(operation.logs, "utf-8"))
