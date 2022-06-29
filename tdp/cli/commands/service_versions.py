# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import click
from sqlalchemy import and_, func
from tabulate import tabulate

from tdp.cli.session import get_session_class
from tdp.core.models import OperationLog, ServiceLog


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

        latest_success_service_version = (
            session.query(
                func.max(ServiceLog.deployment_id).label("deployment_id"),
                ServiceLog.service.label("service"),
                func.substr(ServiceLog.version, 1, 7).label("version"),
            )
            .join(
                OperationLog,
                and_(
                    ServiceLog.deployment_id == OperationLog.deployment_id,
                    OperationLog.operation.like(
                        ServiceLog.service + "\_%", escape="\\"
                    ),
                ),
            )
            .filter(OperationLog.state == "Success")
            .group_by(ServiceLog.service, ServiceLog.version)
            .order_by("deployment_id", "service")
            .all()
        )

        click.echo(
            "Service versions:\n"
            + tabulate(
                latest_success_service_version,
                headers=["deployment_id", "service", "version"],
            )
        )
