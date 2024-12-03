# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Iterable
from typing import Optional

import click
from sqlalchemy import Engine

from tdp.cli.params import database_dsn_option
from tdp.cli.utils import (
    print_deployment,
    print_object,
    print_operations,
    print_table,
)
from tdp.core.entities.operation import OperationName
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
            deployment_plan = dao.get_planned_deployment()
            if deployment_plan:
                _print_deployment(deployment_plan)
            else:
                click.echo("No planned deployment found")
                click.echo("Create a deployment plan using the `tdp plan` command")
            return
        elif last:
            deployment = dao.get_last_deployment()
            if deployment:
                _print_deployment(deployment)
            else:
                click.echo("No deployment found")
                click.echo("Create a deployment plan using the `tdp plan` command")
            return

        # Print a specific operation
        if deployment_id and operation:
            # Try to parse the operation argument as an integer
            try:
                operation_order = int(operation)
                if record := dao.get_operation(deployment_id, operation_order):
                    _print_operation(record)
                    return
            # If the operation argument is not an integer, consider that it is an
            # operation name
            except ValueError:
                try:
                    OperationName.from_str(operation)
                except ValueError:
                    click.echo(f"Operation {operation} is not a valid operation name.")
                    return
                operations = dao.get_operations_by_name(deployment_id, operation)
                if len(operations) > 1:
                    click.secho(
                        f'Multiple operations "{operations[0].operation}" found for '
                        + f"deployment {deployment_id}:",
                        bold=True,
                    )
                    print_operations(operations, filter_out=["logs"])
                    click.echo(
                        "\nUse the operation order to print a specific operation."
                    )
                    return
                elif len(operations) == 1:
                    _print_operation(operations[0])
                    return
            click.echo(
                f'Operation "{operation}" not found for deployment {deployment_id}'
            )
            click.echo(
                "Either the deployment does not exist or the operation is not"
                + " found for the deployment."
            )
            return

        # Print a specific deployment
        if deployment_id:
            deployment = dao.get_deployment(deployment_id)
            if deployment:
                _print_deployment(deployment)
            else:
                click.echo(f"Deployment {deployment_id} does not exist.")
            return

        # Print all deployments
        _print_deployments(dao.get_last_deployments(limit=limit, offset=offset))


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


def _print_operation(operation: OperationModel) -> None:
    """Print an operation in a human readable format.

    Args:
        operation: Operation to print.
    """
    print_object(operation.to_dict(filter_out=["logs"]))
    if operation.logs:
        click.secho("\nLogs", bold=True)
        click.echo(str(operation.logs, "utf-8"))
