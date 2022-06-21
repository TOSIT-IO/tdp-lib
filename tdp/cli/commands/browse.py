# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from pathlib import Path

import click
from sqlalchemy import select
from tabulate import tabulate

from tdp.cli.session import get_session_class
from tdp.core.models import DeploymentLog, OperationLog, ServiceLog
from tdp.core.models.base import keyvalgen

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo


@click.command(short_help="Browse deployment logs")
@click.argument("deployment_id", required=False)
@click.argument("operation", required=False)
@click.option(
    "--sqlite-path",
    envvar="TDP_SQLITE_PATH",
    required=True,
    type=Path,
    help="Path to SQLITE database file",
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
def browse(deployment_id, operation, sqlite_path, limit, offset):
    session_class = get_session_class(sqlite_path)

    if not deployment_id:
        process_deployments_query(session_class, limit, offset)
    else:
        if not operation:
            process_single_deployment_query(session_class, deployment_id)
        else:
            process_operation_query(session_class, deployment_id, operation)


def process_deployments_query(session_class, limit, offset):
    headers = [key for key, _ in keyvalgen(DeploymentLog)]
    query = select(DeploymentLog).order_by(DeploymentLog.id).limit(limit).offset(offset)

    with session_class() as session:
        result = session.execute(query).scalars().fetchall()
        click.echo(
            "Deployments:\n"
            + tabulate(
                [
                    format_deployment_log(deployment_log, headers)
                    for deployment_log in result
                ],
                headers="keys",
            )
        )


def process_single_deployment_query(session_class, deployment_id):
    deployment_headers = [key for key, _ in keyvalgen(DeploymentLog)]
    operation_headers = [key for key, _ in keyvalgen(OperationLog)]
    service_headers = [key for key, _ in keyvalgen(ServiceLog) if key != "deployment"]
    operation_headers.remove("deployment")
    query = (
        select(DeploymentLog)
        .where(DeploymentLog.id == deployment_id)
        .order_by(DeploymentLog.id)
    )

    with session_class() as session:
        deployment_log = session.execute(query).scalar_one_or_none()
        if deployment_log is None:
            raise click.ClickException(f"Deployment id {deployment_id} does not exist")
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
            "Services:\n"
            + tabulate(
                [
                    format_service_log(service_logs, service_headers)
                    for service_logs in deployment_log.services
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


def process_operation_query(session_class, deployment_id, operation):
    headers = [key for key, _ in keyvalgen(OperationLog) if key != "deployment"]
    service_headers = [key for key, _ in keyvalgen(ServiceLog) if key != "deployment"]
    query = (
        select(OperationLog)
        .where(OperationLog.deployment_id == deployment_id)
        .where(OperationLog.operation == operation)
        .order_by(OperationLog.start)
    )
    with session_class() as session:
        operation_log = session.execute(query).scalar_one_or_none()
        if operation_log is None:
            raise click.ClickException(
                f"Operation {operation} does exist in deployment {deployment_id}"
            )

        click.echo(
            "Service:\n"
            + tabulate(
                [
                    format_service_log(service_log, service_headers)
                    for service_log in operation_log.deployment.services
                    if service_log.service == operation_log.operation.split("_")[0]
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


def format_deployment_log(deployment_log, headers):
    def custom_format(key, value):
        if key in ["sources", "targets"] and value is not None:
            if len(value) > 2:
                return value[0] + ",...," + value[-1]
            else:
                return ",".join(value)
        elif key == "operations":
            if len(value) > 2:
                return value[0].operation + ",...," + value[-1].operation
            else:
                return ",".join(operation_log.operation for operation_log in value)
        elif key == "services":
            return ",".join(str(service_log.service) for service_log in value)
        elif isinstance(value, datetime):
            return translate_timezone(value)
        else:
            return str(value)

    return {key: custom_format(key, getattr(deployment_log, key)) for key in headers}


def format_operation_log(operation_log, headers):
    def custom_format(key, value):
        if key == "logs":
            return str(value[:40])
        elif isinstance(value, datetime):
            return translate_timezone(value)
        else:
            return str(value)

    return {key: custom_format(key, getattr(operation_log, key)) for key in headers}


def format_service_log(service_log, headers):
    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        else:
            return str(value)

    return {key: custom_format(key, getattr(service_log, key)) for key in headers}
