# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Iterable

import click

from tdp.cli.queries import (
    get_deployment,
    get_deployments,
    get_operation_log,
    get_planned_deployment_log,
)
from tdp.cli.session import get_session
from tdp.cli.utils import database_dsn, print_deployment, print_object, print_table
from tdp.core.models import DeploymentLog, OperationLog


@click.command(short_help="Browse deployment logs")
@click.argument("deployment_id", required=False)
@click.argument("operation", required=False)
@click.option(
    "-p",
    "--plan",
    is_flag=True,
    help="Print the planned deployment, if exist.",
)
@click.option(
    "--limit",
    envvar="TDP_LIMIT",
    type=int,
    default=15,
    help="Limit number of deployments returned",
)
@click.option(
    "--offset",
    envvar="TDP_OFFSET",
    type=int,
    default=0,
    help="At which offset should the database query should start",
)
@database_dsn
def browse(
    deployment_id: int,
    operation: str,
    plan: bool,
    limit: int,
    offset: int,
    database_dsn: str,
):
    try:
        with get_session(database_dsn) as session:
        
            # Print last deployment plan
            if plan:
                deployment_plan = get_planned_deployment_log(session)
                if deployment_plan:
                    _print_deployment(deployment_plan)
                else:
                    click.echo("No planned deployment found")
                    click.echo("Create a deployment plan using the `tdp plan` command")
                return

            # Print a specific operation
            if deployment_id and operation:
                _print_operation(get_operation_log(session, deployment_id, operation))
                return

            # Print a specific deployment
            if deployment_id:
                _print_deployment(get_deployment(session, deployment_id))
                return

            # Print all deployments
            _print_deployments(get_deployments(session, limit, offset))

    except Exception as e:
        raise click.ClickException(e)


def _print_deployments(deployments: Iterable[DeploymentLog]) -> None:
    """Print a list of deployments in a human readable format.

    Args:
        deployments: List of deployments to print
    """
    if not deployments:
        click.echo("No deployment found")
        click.echo("Create a deployment plan using the `tdp plan` command")

    print_table(
        [d.to_dict(filter_out=["options"]) for d in deployments],
    )


def _print_deployment(deployment: DeploymentLog) -> None:
    """Print a deployment in a human readable format.

    Args:
        deployment: Deployment to print
    """
    if not deployment:
        click.echo("Deployment does not exist.")
        return

    print_deployment(deployment)


def _print_operation(operation: OperationLog) -> None:
    """Print an operation in a human readable format.

    Args:
        operation: Operation to print
    """
    # Print general operation infos
    click.secho("Operation details", bold=True)
    click.echo(print_object(operation.to_dict()))

    # Print related component version logs
    if operation.deployment.component_version:
        click.secho("\nAffected hosts", bold=True)
        print_table(
            [
                c.to_dict()
                for c in operation.deployment.component_version
                if c.service == operation.operation.split("_")[0]
            ],
        )

    # Print operation logs
    if operation.logs:
        click.secho("\nOperation logs", bold=True)
        click.echo(str(operation.logs, "utf-8"))
    pass
