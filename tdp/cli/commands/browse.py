# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from enum import Enum
from typing import List

import click
from tabulate import tabulate

from .utils import (
    execute_get_deployment_query,
    execute_get_deployments_query,
    execute_get_operation_log_query,
)
from tdp.cli.session import get_session_class
from tdp.cli.utils import database_dsn
from tdp.core.models import DeploymentLog, OperationLog, ServiceComponentLog
from tdp.core.models.base import keyvalgen

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo


@click.command(short_help="Browse deployment logs")
@click.argument("deployment_id", required=False)
@click.argument("operation", required=False)
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
def browse(deployment_id, operation, limit, offset, database_dsn):
    session_class = get_session_class(database_dsn)
    with session_class() as session:
        try:
            if not deployment_id:
                deployments = execute_get_deployments_query(session, limit, offset)
                print_formatted_deployments(deployments)
            else:
                if not operation:
                    deployment = execute_get_deployment_query(session, deployment_id)
                    print_formatted_deployment(deployment)
                else:
                    operation_log = execute_get_operation_log_query(
                        session, deployment_id, operation
                    )
                    print_formatted_operation_log(operation_log)
        except Exception as e:
            raise click.ClickException(str(e)) from e


def print_formatted_deployments(deployments: List[DeploymentLog]) -> None:
    """Prints a list of deployments in a formatted table.

    Args:
        deployments: List of deployments to print.

    Examples:
        >>> print_formatted_deployments([DeploymentLog(...)])
        Deployments:
        id  state  created_at  updated_at  sources  targets  service_components
        --  -----  ----------  ----------  -------  -------  -------------------
        1   FAILED  2021-01-01  2021-01-01  None     None     None
    """
    headers = DeploymentLog.__table__.columns.keys() + [
        str(DeploymentLog.service_components).split(".")[1]
    ]
    click.echo(
        "Deployments:\n"
        + tabulate(
            [
                format_deployment_log(deployment_log, headers)
                for deployment_log in deployments
            ],
            headers="keys",
        )
    )


def print_formatted_deployment(deployment_log: DeploymentLog) -> None:
    """Prints a deployment in a formatted table.

    Args:
        deployment_log: Deployment to print.

    Examples:
        >>> print_formatted_deployment(DeploymentLog(...))
        Deployment:
        id  state  created_at  updated_at  sources  targets  service_components
        --  -----  ----------  ----------  -------  -------  -------------------
        1   FAILED  2021-01-01  2021-01-01  None     None     None
    """
    deployment_headers = [key for key, _ in keyvalgen(DeploymentLog)]
    operation_headers = OperationLog.__table__.columns.keys()
    service_headers = ServiceComponentLog.__table__.columns.keys()

    sources = deployment_log.sources or ["None"]
    targets = deployment_log.targets or ["None"]
    click.echo(
        "Deployment:\n"
        + tabulate(
            [format_deployment_log(deployment_log, deployment_headers)],
            headers="keys",
        )
    )
    click.echo("Sources:\n  " + tabulate({"source": sources}, headers="keys"))
    click.echo("Targets:\n  " + tabulate({"target": targets}, headers="keys"))
    click.echo(
        "Service Component logs:\n"
        + tabulate(
            [
                format_service_component_log(service_logs, service_headers)
                for service_logs in deployment_log.service_components
            ],
            headers="keys",
        )
    )
    click.echo(
        "Operations:\n"
        + tabulate(
            [
                format_operation_log(operation_log, operation_headers)
                for operation_log in deployment_log.operations
            ],
            headers="keys",
        )
    )


def print_formatted_operation_log(operation_log: OperationLog) -> None:
    """Prints an operation log in a formatted table.

    Args:
        operation_log: Operation log to print.

    Examples:
        >>> print_formatted_operation_log(OperationLog(...))
        Service:
        id  service  component  version  created_at  updated_at
        --  -------  ---------  -------  ----------  ----------
        1   service  component  1.0.0    2021-01-01  2021-01-01
        Operation:
        id  deployment_id  operation  state  created_at  updated_at  logs
        --  -------------  ---------  -----  ----------  ----------  ----
        1   1              operation  FAILED  2021-01-01  2021-01-01  None
    """
    headers = OperationLog.__table__.columns.keys()
    service_headers = ServiceComponentLog.__table__.columns.keys()
    # TODO: this outputs Service and ServiceComponent version when it should
    # only output a ServiceComponent when service_component_log.component is not None
    click.echo(
        "Service:\n"
        + tabulate(
            [
                format_service_component_log(service_component_log, service_headers)
                for service_component_log in operation_log.deployment.service_components
                if service_component_log.service
                == operation_log.operation.split("_")[0]
            ],
            headers="keys",
        )
    )
    click.echo(
        "Operation:\n"
        + tabulate(
            [format_operation_log(operation_log, headers)],
            headers="keys",
        )
    )
    if operation_log.logs:
        click.echo(
            f"{operation_log.operation} logs:\n" + str(operation_log.logs, "utf-8")
        )


def translate_timezone(timestamp: datetime) -> datetime:
    """Translates a timestamp from UTC to the local timezone.

    Args:
        timestamp: Timestamp to translate.

    Returns:
        Translated timestamp.
    """
    return timestamp.replace(tzinfo=timezone.utc).astimezone(LOCAL_TIMEZONE)


def format_service_component(service_component_log: ServiceComponentLog) -> str:
    """Formats a service component log into a string.

    Args:
        service_component_log: Service component log to format.

    Returns:
        Formatted service component log.

    Examples:
        >>> format_service_component(ServiceComponentLog(...))
        "service_component"
    """
    if service_component_log.component is None:
        return service_component_log.service
    return f"{service_component_log.service}_{service_component_log.component}"


def format_deployment_log(deployment_log: DeploymentLog, headers: List[str]) -> dict:
    """Formats a deployment log into a dictionary.

    Args:
        deployment_log: Deployment log to format.
        headers: Headers to include in the formatted deployment log.

    Returns:
        Formatted deployment log.

    Examples:
        >>> format_deployment_log(DeploymentLog(...), ["id", "state"])
        {"id": 1, "state": "FAILED"}
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
        elif key == "service_components":
            if len(value) > 2:
                return (
                    format_service_component(value[0])
                    + ",...,"
                    + format_service_component(value[-1])
                )
            return ",".join(
                format_service_component(service_component_log)
                for service_component_log in value
            )
        elif isinstance(value, datetime):
            return translate_timezone(value)
        elif isinstance(value, Enum):
            return value.name
        else:
            return str(value)

    return {key: custom_format(key, getattr(deployment_log, key)) for key in headers}


def format_operation_log(operation_log: OperationLog, headers: List[str]) -> dict:
    """Formats an operation log into a dictionary.

    Args:
        operation_log: Operation log to format.
        headers: Headers to include in the formatted operation log.

    Returns:
        Formatted operation log.

    Examples:
        >>> format_operation_log(OperationLog(...), ["id", "operation"])
        {"id": 1, "operation": "operation"}
    """

    def custom_format(key, value):
        if key == "logs":
            return str(value[:40])
        elif isinstance(value, datetime):
            return translate_timezone(value)
        elif isinstance(value, Enum):
            return value.name
        else:
            return str(value)

    return {key: custom_format(key, getattr(operation_log, key)) for key in headers}


def format_service_component_log(
    service_component_log: ServiceComponentLog, headers: List[str]
) -> dict:
    """Formats a service component log into a dictionary.

    Args:
        service_component_log: Service component log to format.
        headers: Headers to include in the formatted service component log.

    Returns:
        Formatted service component log.

    Examples:
        >>> format_service_component_log(ServiceComponentLog(...), ["id", "version"])
        {"id": 1, "version": "1.0.0"}
    """

    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        else:
            return str(value)

    return {
        key: custom_format(key, getattr(service_component_log, key)) for key in headers
    }
