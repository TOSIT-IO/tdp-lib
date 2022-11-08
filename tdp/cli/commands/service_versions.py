# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from tabulate import tabulate

from tdp.cli.commands.queries import get_latest_success_service_component_version_query
from tdp.cli.session import get_session_class
from tdp.core.models import ServiceComponentLog


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
        latest_success_service_version = session.execute(
            get_latest_success_service_component_version_query()
        ).all()

        click.echo(
            "Service versions:\n"
            + tabulate(
                latest_success_service_version,
                headers=[
                    ServiceComponentLog.deployment_id.name,
                    ServiceComponentLog.service.name,
                    ServiceComponentLog.component.name,
                    ServiceComponentLog.version.name,
                ],
            )
        )
