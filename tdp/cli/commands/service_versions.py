# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from sqlalchemy import and_, desc, func, select, tuple_
from tabulate import tabulate

from tdp.cli.session import get_session_class
from tdp.core.models import OperationLog, ServiceLog
from tdp.core.runner.executor import StateEnum


@click.command(
    short_help=(
        "Get the version of deployed services."
        "(If a service has never been deployed, does not show it)"
    )
)
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
def service_versions(database_dsn):
    session_class = get_session_class(database_dsn)

    with session_class() as session:
        max_depid_label = f"max_{ServiceLog.deployment_id.name}"

        latest_success_for_each_service = (
            select(
                func.max(ServiceLog.deployment_id).label(max_depid_label),
                ServiceLog.service,
            )
            .join(
                OperationLog,
                and_(
                    ServiceLog.deployment_id == OperationLog.deployment_id,
                    OperationLog.operation.like(
                        ServiceLog.service + "\\_%", escape="\\"
                    ),
                ),
            )
            .filter(OperationLog.state == StateEnum.SUCCESS.value)
            .group_by(ServiceLog.service)
        )

        service_logs_matching_subquery = (
            session.query(
                ServiceLog.deployment_id,
                ServiceLog.service,
                func.substr(ServiceLog.version, 1, 7),
            )
            .filter(
                tuple_(ServiceLog.deployment_id, ServiceLog.service).in_(
                    latest_success_for_each_service
                )
            )
            .order_by(desc(ServiceLog.deployment_id))
            .all()
        )

        click.echo(
            "Service versions:\n"
            + tabulate(
                service_logs_matching_subquery,
                headers=[
                    ServiceLog.deployment_id.name,
                    ServiceLog.service.name,
                    ServiceLog.version.name,
                ],
            )
        )
