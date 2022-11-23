# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from enum import Enum

import click
from sqlalchemy import select
from sqlalchemy.orm import joinedload
from tabulate import tabulate

from tdp.cli.commands.queries import get_deployment
from tdp.cli.session import get_session_class
from tdp.core.models import DeploymentLog, OperationLog, ServiceComponentLog
from tdp.core.models.base import keyvalgen

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo


@click.command(short_help="Browse deployment logs")
@click.argument("deployment_id", required=False)
@click.argument("operation", required=False)
@click.option(
    "--database-dsn",
    envvar="TDP_DATABASE_DSN",
    required=True,
    type=str,
    help=(
        "Database Data Source Name, in sqlalchemy driver form "
        "example: sqlite:////data/tdp.db or sqlite+pysqlite:////data/tdp.db. "
        "You might need to install the relevant driver to your installation (such "
        "as psycopg2 for postgresql)"
    ),
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
def browse(deployment_id, operation, database_dsn, limit, offset):
    session_class = get_session_class(database_dsn)
    try:
        if not deployment_id:
            print_formatted_deployments(get_deployments(session_class, limit, offset))
        else:
            if not operation:
                print_formatted_deployment(get_deployment(session_class, deployment_id))
            else:
                print_formatted_operation_log(
                    get_operation_log(session_class, deployment_id, operation)
                )
    except Exception as e:
        raise click.ClickException(str(e)) from e


def get_deployments(session_class, limit, offset):
    query = (
        select(DeploymentLog)
        .options(joinedload(DeploymentLog.service_components))
        .order_by(DeploymentLog.id)
        .limit(limit)
        .offset(offset)
    )
    with session_class() as session:
        return session.execute(query).unique().scalars().fetchall()


def get_operation_log(session_class, deployment_id, operation):
    query = (
        select(OperationLog)
        .options(
            joinedload(OperationLog.deployment),
            joinedload("deployment.service_components"),
        )
        .where(OperationLog.deployment_id == deployment_id)
        .where(OperationLog.operation == operation)
        .order_by(OperationLog.start_time)
    )
    with session_class() as session:
        operation_log = session.execute(query).unique().scalar_one_or_none()
        if operation_log is None:
            raise ValueError(
                f"Operation {operation} does exist in deployment {deployment_id}"
            )
        return operation_log


def print_formatted_deployments(deployments):
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


def print_formatted_deployment(deployment_log):
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


def print_formatted_operation_log(operation_log):
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


def translate_timezone(timestamp):
    return timestamp.replace(tzinfo=timezone.utc).astimezone(LOCAL_TIMEZONE)


def format_service_component(service_component_log):
    if service_component_log.component is None:
        return service_component_log.service
    return f"{service_component_log.service}_{service_component_log.component}"


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


def format_operation_log(operation_log, headers):
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


def format_service_component_log(service_component_log, headers):
    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        else:
            return str(value)

    return {
        key: custom_format(key, getattr(service_component_log, key)) for key in headers
    }
