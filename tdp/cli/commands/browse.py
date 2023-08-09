# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from enum import Enum

import click
from tabulate import tabulate

from tdp.cli.queries import (
    get_deployment,
    get_deployments,
    get_operation_log,
    get_planned_deployment_log,
)
from tdp.cli.session import get_session
from tdp.cli.utils import database_dsn
from tdp.core.models import ComponentVersionLog, DeploymentLog, OperationLog

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo


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
    with get_session(database_dsn) as session:
        if plan:
            deployment_plan = get_planned_deployment_log(session)
            if deployment_plan:
                _print_formatted_deployment(deployment_plan)
            else:
                print("No deployment plan to show.")
        else:
            if not deployment_id:
                _print_formatted_deployments(get_deployments(session, limit, offset))
            else:
                if not operation:
                    _print_formatted_deployment(get_deployment(session, deployment_id))
                else:
                    _print_formatted_operation_log(
                        get_operation_log(session, deployment_id, operation)
                    )


def _print_formatted_deployments(deployments: list[DeploymentLog]) -> None:
    """Print a list of deployment logs in a human readable format.

    Args:
        deployments: List of deployment logs to print

    Raises:
        ClickException: If the deployment log is not found
    """
    headers = DeploymentLog.__table__.columns.keys() + [
        str(DeploymentLog.component_version).split(".")[1]
    ]
    click.echo(
        "Deployments:\n"
        + tabulate(
            [
                _format_deployment_log(deployment_log, headers)
                for deployment_log in deployments
            ],
            headers="keys",
        )
    )


def _print_formatted_deployment(deployment_log: DeploymentLog) -> None:
    """Print a deployment log in a human readable format.

    Args:
        deployment_log: Deployment log to print

    Raises:
        ClickException: If the deployment log is not found
    """
    deployment_headers = DeploymentLog.__table__.columns.keys()
    operation_headers = OperationLog.__table__.columns.keys()
    service_headers = ComponentVersionLog.__table__.columns.keys()

    click.echo(
        "Deployment:\n"
        + tabulate(
            _format_deployment_log(deployment_log, deployment_headers).items(),
            ["Property", "Value"],
            colalign=("right",),
        )
    )
    if deployment_log.component_version:
        click.echo(
            "\Component verion logs:\n"
            + tabulate(
                [
                    _format_component_version_log(service_logs, service_headers)
                    for service_logs in deployment_log.component_version
                ],
                headers="keys",
            )
        )
    if deployment_log.operations:
        click.echo(
            "\nOperations:\n"
            + tabulate(
                [
                    _format_operation_log(operation_log, operation_headers)
                    for operation_log in deployment_log.operations
                ],
                headers="keys",
            )
        )


def _print_formatted_operation_log(operation_log: OperationLog) -> None:
    """Print an operation log in a human readable format.

    Args:
        operation_log: Operation log to print

    Raises:
        ClickException: If the operation log is not found
    """
    headers = OperationLog.__table__.columns.keys()
    service_headers = ComponentVersionLog.__table__.columns.keys()
    # TODO: this outputs Service and ComponentVersionLog when it should
    # only output a ComponentVersionLog when component_version_log.component is not None
    click.echo(
        "Service:\n"
        + tabulate(
            [
                _format_component_version_log(component_version_log, service_headers)
                for component_version_log in operation_log.deployment.component_version
                if component_version_log.service
                == operation_log.operation.split("_")[0]
            ],
            headers="keys",
        )
    )
    click.echo(
        "Operation:\n"
        + tabulate(
            [_format_operation_log(operation_log, headers)],
            headers="keys",
        )
    )
    if operation_log.logs:
        click.echo(
            f"{operation_log.operation} logs:\n" + str(operation_log.logs, "utf-8")
        )


def _translate_timezone(timestamp: datetime) -> datetime:
    """Translate a timestamp from UTC to the local timezone."""
    return timestamp.replace(tzinfo=timezone.utc).astimezone(LOCAL_TIMEZONE)


def _format_component_name(component_version_log: ComponentVersionLog) -> str:
    """Format a component log into a string.

    Args:
        component_version_log: Service component log to format

    Returns:
        <service>_<component> string or <service> string.
    """
    if component_version_log.component is None:
        return component_version_log.service
    return f"{component_version_log.service}_{component_version_log.component}"


def _format_deployment_log(deployment_log: DeploymentLog, headers: list[str]) -> dict:
    """Format a deployment log into a dict for tabulate.

    Args:
        deployment_log: Deployment log to format
        headers: Headers to include in the formatted log

    Returns:
        A dict with the formatted DeploymentLog object.
    """

    def custom_format(key, value):
        if key in ["sources", "targets"] and value is not None:
            if len(value) > 2:
                return value[0] + ",...," + value[-1]
            return ",".join(value)
        elif key == "operations":
            if len(value) > 2:
                return value[0].operation + ",...," + value[-1].operation
            return ",".join(operation_log.operation for operation_log in value)
        elif key == "component_version":
            if len(value) > 2:
                return (
                    _format_component_name(value[0])
                    + ",...,"
                    + _format_component_name(value[-1])
                )
            return ",".join(
                _format_component_name(component_version_log)
                for component_version_log in value
            )
        elif isinstance(value, datetime):
            return _translate_timezone(value)
        elif isinstance(value, Enum):
            return value.name
        else:
            return str(value)

    return {key: custom_format(key, getattr(deployment_log, key)) for key in headers}


def _format_operation_log(operation_log: OperationLog, headers: list[str]) -> dict:
    """Format an OperationLog object into a dict for tabulate.

    Args:
        operation_log: The OperationLog object to format.
        headers: The headers to use for the tabulate table.

    Returns:
        A dict with the formatted OperationLog object.
    """

    def custom_format(key, value):
        if key == "logs":
            return "" if value is None else str(value[:40])
        elif isinstance(value, datetime):
            return _translate_timezone(value)
        elif isinstance(value, Enum):
            return value.name
        else:
            return str(value)

    return {key: custom_format(key, getattr(operation_log, key)) for key in headers}


def _format_component_version_log(
    component_version_log: ComponentVersionLog, headers: list[str]
) -> dict:
    """Format a ComponentVersionLog object into a dict for tabulate.

    Args:
        component_version_log: The ComponentVersionLog object to format.
        headers: The headers to use for the tabulate table.

    Returns:
        A dict with the formatted ComponentVersionLog object.
    """

    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        else:
            return str(value)

    return {
        key: custom_format(key, getattr(component_version_log, key)) for key in headers
    }
