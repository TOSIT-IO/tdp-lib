# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from tabulate import tabulate

from tdp.cli.queries import get_latest_success_component_version_log
from tdp.cli.session import get_session
from tdp.cli.utils import database_dsn


@click.command(
    short_help=(
        "Get the version of deployed services."
        "(If a service has never been deployed, does not show it)"
    )
)
@database_dsn
def service_versions(database_dsn):
    try:
        with get_session(database_dsn) as session:
            latest_success_service_version_logs = (
                get_latest_success_component_version_log(session)
            )
            if not any(latest_success_service_version_logs):
                click.echo("No service has been deployed.")
                return

            click.echo(
                tabulate(
                    [
                        c.to_dict(filter_out=["id"])
                        for c in latest_success_service_version_logs
                    ],
                    headers="keys",
                )
            )

    except Exception as e:
        raise click.ClickException(e)
