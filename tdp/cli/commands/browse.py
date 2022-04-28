# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from pathlib import Path

import click
from sqlalchemy import select
from tabulate import tabulate

from tdp.cli.session import get_session_class
from tdp.core.models import ActionLog, DeploymentLog, ServiceLog
from tdp.core.models.base import keyvalgen

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo


@click.command(short_help="Browse deployment logs")
@click.argument("deployment_id", required=False)
@click.argument("action", required=False)
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
def browse(deployment_id, action, sqlite_path, limit, offset):
    session_class = get_session_class(sqlite_path)

    if not deployment_id:
        process_deployments_query(session_class, limit, offset)
    else:
        if not action:
            process_single_deployment_query(session_class, deployment_id)
        else:
            process_action_query(session_class, deployment_id, action)


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
    action_headers = [key for key, _ in keyvalgen(ActionLog)]
    service_headers = [key for key, _ in keyvalgen(ServiceLog) if key != "deployment"]
    action_headers.remove("deployment")
    query = (
        select(DeploymentLog)
        .where(DeploymentLog.id == deployment_id)
        .order_by(DeploymentLog.id)
    )

    with session_class() as session:
        result = session.execute(query).scalars().fetchall()
        sources = result[0].sources or ["None"]
        targets = result[0].targets or ["None"]
        click.echo(
            "Deployment:\n"
            + tabulate(
                [
                    format_deployment_log(deployment_log, deployment_headers)
                    for deployment_log in result
                ],
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
                    for deployment_log in result
                    for service_logs in deployment_log.services
                ],
                headers="keys",
            )
        )
        click.echo(
            "Actions:\n"
            + tabulate(
                [
                    format_action_log(action_log, action_headers)
                    for action_log in result[0].actions
                ],
                headers="keys",
            )
        )


def process_action_query(session_class, deployment_id, action):
    headers = [key for key, _ in keyvalgen(ActionLog) if key != "deployment"]
    service_headers = [key for key, _ in keyvalgen(ServiceLog) if key != "deployment"]
    query = (
        select(ActionLog)
        .where(ActionLog.deployment_id == deployment_id)
        .where(ActionLog.action == action)
        .order_by(ActionLog.start)
    )
    with session_class() as session:
        result = session.execute(query).scalars().fetchall()
        action_logs = [action_log for action_log in result]
        click.echo(
            "Service:\n"
            + tabulate(
                [
                    format_service_log(service_log, service_headers)
                    for action_log in result
                    for service_log in action_log.deployment.services
                    if service_log.service == action_log.action.split("_")[0]
                ],
                headers="keys",
            )
        )
        click.echo(
            "Action:\n"
            + tabulate(
                [format_action_log(action_log, headers) for action_log in action_logs],
                headers="keys",
            )
        )
        if action_logs:
            action_log = action_logs[0]
            click.echo(f"{action_log.action} logs:\n" + str(action_log.logs, "utf-8"))


def translate_timezone(timestamp):
    return timestamp.replace(tzinfo=timezone.utc).astimezone(LOCAL_TIMEZONE)


def format_deployment_log(deployment_log, headers):
    def custom_format(key, value):
        if key in ["sources", "targets"] and value is not None:
            if len(value) > 2:
                return value[0] + ",...," + value[-1]
            else:
                return ",".join(value)
        elif key == "actions":
            if len(value) > 2:
                return value[0].action + ",...," + value[-1].action
            else:
                return ",".join(action.action for action in value)
        elif key == "services":
            return ",".join(str(service_log.service) for service_log in value)
        elif isinstance(value, datetime):
            return translate_timezone(value)
        else:
            return str(value)

    return {key: custom_format(key, getattr(deployment_log, key)) for key in headers}


def format_action_log(action_log, headers):
    def custom_format(key, value):
        if key == "logs":
            return str(value[:40])
        elif isinstance(value, datetime):
            return translate_timezone(value)
        else:
            return str(value)

    return {key: custom_format(key, getattr(action_log, key)) for key in headers}


def format_service_log(service_log, headers):
    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        else:
            return str(value)

    return {key: custom_format(key, getattr(service_log, key)) for key in headers}
