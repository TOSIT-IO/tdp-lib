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
from tdp.cli.session import get_session_class
from tdp.cli.utils import database_dsn
from tdp.core.models import DeploymentLog, OperationLog, ComponentVersionLog
from tdp.core.models.base import keyvalgen

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
    session_class = get_session_class(database_dsn)
    with session_class() as session:
        try:
            if plan:
                deployment_plan = get_planned_deployment_log(session)
                if deployment_plan:
                    print_formatted_deployment(deployment_plan)
                else:
                    print("No deployment plan to show.")
            else:
                if not deployment_id:
                    print_formatted_deployments(get_deployments(session, limit, offset))
                else:
                    if not operation:
                        print_formatted_deployment(
                            get_deployment(session, deployment_id)
                        )
                    else:
                        print_formatted_operation_log(
                            get_operation_log(session, deployment_id, operation)
                        )
        except Exception as e:
            raise click.ClickException(str(e)) from e


def print_formatted_deployments(deployments):
    headers = DeploymentLog.__table__.columns.keys() + [
        str(DeploymentLog.component_version).split(".")[1]
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


def print_formatted_deployment(deployment_log):
    deployment_headers = [key for key, _ in keyvalgen(DeploymentLog)]
    operation_headers = OperationLog.__table__.columns.keys()
    service_headers = ComponentVersionLog.__table__.columns.keys()

    click.echo(
        "Deployment:\n"
        + tabulate(
            format_deployment_log(deployment_log, deployment_headers).items(),
            ["Property", "Value"],
            colalign=("right",),
        )
    )
    if deployment_log.component_version:
        click.echo(
            "\Component verion logs:\n"
            + tabulate(
                [
                    format_component_version_log(service_logs, service_headers)
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
                    format_operation_log(operation_log, operation_headers)
                    for operation_log in deployment_log.operations
                ],
                headers="keys",
            )
        )


def print_formatted_operation_log(operation_log):
    headers = OperationLog.__table__.columns.keys()
    service_headers = ComponentVersionLog.__table__.columns.keys()
    # TODO: this outputs Service and ComponentVersionLog when it should
    # only output a ComponentVersionLog when component_version_log.component is not None
    click.echo(
        "Service:\n"
        + tabulate(
            [
                format_component_version_log(component_version_log, service_headers)
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
            [format_operation_log(operation_log, headers)],
            headers="keys",
        )
    )
    if operation_log.logs:
        click.echo(
            f"{operation_log.operation} logs:\n" + str(operation_log.logs, "utf-8")
        )


def translate_timezone(timestamp):
    return timestamp.replace(tzinfo=timezone.utc).astimezone(LOCAL_TIMEZONE)


def format_service_component(component_version_log):
    if component_version_log.component is None:
        return component_version_log.service
    return f"{component_version_log.service}_{component_version_log.component}"


def format_deployment_log(deployment_log, headers):
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
                    format_service_component(value[0])
                    + ",...,"
                    + format_service_component(value[-1])
                )
            return ",".join(
                format_service_component(component_version_log)
                for component_version_log in value
            )
        elif isinstance(value, datetime):
            return translate_timezone(value)
        elif isinstance(value, Enum):
            return value.name
        else:
            return str(value)

    return {key: custom_format(key, getattr(deployment_log, key)) for key in headers}


def format_operation_log(operation_log, headers):
    def custom_format(key, value):
        if key == "logs":
            return "" if value is None else str(value[:40])
        elif isinstance(value, datetime):
            return translate_timezone(value)
        elif isinstance(value, Enum):
            return value.name
        else:
            return str(value)

    return {key: custom_format(key, getattr(operation_log, key)) for key in headers}


def format_component_version_log(component_version_log, headers):
    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        else:
            return str(value)

    return {
        key: custom_format(key, getattr(component_version_log, key)) for key in headers
    }
