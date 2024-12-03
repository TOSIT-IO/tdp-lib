# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

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
        if plan and last:
            raise click.BadOptionUsage(
                "--plan", "Cannot use --plan and --last together."
            )
        if deployment_id and (plan or last):
            raise click.BadOptionUsage(
                "deployment_id", "Cannot use deployment_id with --plan or --last."
            )
        # Print the planned deployment
        if plan:
            browse_plan(dao)
            return
        # Print the last deployment
        elif last:
            browse_last(dao)
            return
        # Print a specific deployment
        elif deployment_id and not operation:
            browse_deployment(dao, deployment_id)
            return
        # Print a specific operation
        elif deployment_id and operation:
            browse_operation(dao, deployment_id, operation)
            return
        # Print all deployments
        else:
            browse_deployments(dao, limit, offset)


def browse_plan(dao: Dao) -> None:
    if deployment_plan := dao.get_planned_deployment():
        print_deployment(deployment_plan, filter_out=["logs"])
    else:
        click.echo("No planned deployment found")
        click.echo("Create a deployment plan using the `tdp plan` command")


def browse_last(dao: Dao) -> None:
    if last_deployment := dao.get_last_deployment():
        print_deployment(last_deployment, filter_out=["logs"])
    else:
        click.echo("No deployment found")
        click.echo("Create a deployment plan using the `tdp plan` command")


def browse_deployment(dao: Dao, deployment_id: int) -> None:
    if deployment := dao.get_deployment(deployment_id):
        print_deployment(deployment, filter_out=["logs"])
    else:
        click.echo(f"Deployment {deployment_id} does not exist.")


def browse_operation(dao: Dao, deployment_id: int, operation: str) -> None:
    record = None
    # Try to parse the operation argument as an integer
    try:
        operation_order = int(operation)
        record = dao.get_operation(deployment_id, operation_order)
    # If the operation argument is not an integer, consider that it is an
    # operation name
    except ValueError:
        # Check if the operation name is valid
        try:
            OperationName.from_str(operation)
        except ValueError as e:
            raise click.BadParameter(
                f"Operation {operation} is not a valid operation name."
            ) from e
        operations = dao.get_operations_by_name(deployment_id, operation)
        if len(operations) == 1:
            record = operations[0]
        # If there are multiple operations with the given name, print them asking for a
        # specific operation order
        elif len(operations) > 1:
            click.secho(
                f'Multiple operations "{operations[0].operation}" found for '
                + f"deployment {deployment_id}:",
                bold=True,
            )
            print_operations(operations, filter_out=["logs"])
            click.echo("\nUse the operation order to print a specific operation.")
            return
    if record:
        print_object(record.to_dict(filter_out=["logs"]))
        if record.logs:
            click.secho("\nLogs", bold=True)
            click.echo(str(record.logs, "utf-8"))
    else:
        click.echo(f'Operation "{operation}" not found for deployment {deployment_id}')
        click.echo(
            "Either the deployment does not exist or the operation is not"
            + " found for the deployment."
        )


def browse_deployments(dao: Dao, limit: int, offset: int) -> None:
    deployments = dao.get_last_deployments(limit=limit, offset=offset)
    if len(deployments) > 0:
        print_table(
            [d.to_dict(filter_out=["options"]) for d in deployments],
        )
    else:
        click.echo("No deployments found.")
        click.echo("Create a deployment plan using the `tdp plan` command.")
