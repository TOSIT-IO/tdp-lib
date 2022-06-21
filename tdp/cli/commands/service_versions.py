# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import click
from sqlalchemy import func, select, tuple_
from tabulate import tabulate

from tdp.cli.session import get_session_class
from tdp.core.models import ServiceLog
from tdp.core.models.base import keyvalgen


@click.command(
    short_help=(
        "Get the version of deployed services."
        "(If a service has never been deployed, does not show it)"
    )
)
@click.option(
    "--sqlite-path",
    envvar="TDP_SQLITE_PATH",
    required=True,
    type=Path,
    help="Path to SQLITE database file",
)
def service_versions(sqlite_path):
    session_class = get_session_class(sqlite_path)
    with session_class() as session:
        service_headers = [
            key for key, _ in keyvalgen(ServiceLog) if key != "deployment"
        ]

        latest_deployment_by_service = select(
            func.max(ServiceLog.deployment_id), ServiceLog.service
        ).group_by(ServiceLog.service)

        service_latest_version = (
            session.query(ServiceLog)
            .order_by(ServiceLog.deployment_id.desc())
            .filter(
                tuple_(ServiceLog.deployment_id, ServiceLog.service).in_(
                    latest_deployment_by_service
                )
            )
            .all()
        )

        click.echo(
            "Service versions:\n"
            + tabulate(
                [
                    format_service_log(service_log, service_headers)
                    for service_log in service_latest_version
                ],
                headers="keys",
            )
        )


def format_service_log(service_log, headers):
    def custom_format(key, value):
        if key == "version":
            return str(value[:7])
        else:
            return str(value)

    return {key: custom_format(key, getattr(service_log, key)) for key in headers}
