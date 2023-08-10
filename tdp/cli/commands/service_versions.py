# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from tabulate import tabulate

from tdp.cli.commands.browse import _format_component_version_log
from tdp.cli.queries import get_latest_success_component_version_log
from tdp.cli.session import get_session_class
from tdp.cli.utils import database_dsn
from tdp.core.models import ComponentVersionLog


@click.command(
    short_help=(
        "Get the version of deployed services."
        "(If a service has never been deployed, does not show it)"
    )
)
@database_dsn
def service_versions(database_dsn):
    session_class = get_session_class(database_dsn)

    with session_class() as session:
        latest_success_service_version_logs = get_latest_success_component_version_log(
            session
        )

        if any(latest_success_service_version_logs):
            # TODO: refactor so that this not import private method from browse.py
            _print_component_version_logs(latest_success_service_version_logs)
        else:
            click.echo("No service has been deployed.")


def _print_component_version_logs(component_version_logs: list[ComponentVersionLog]):
    component_version_log_headers = ComponentVersionLog.__table__.columns.keys()

    click.echo(
        "Service versions:\n"
        + tabulate(
            [
                _format_component_version_log(
                    component_version_log, component_version_log_headers
                )
                for component_version_log in component_version_logs
            ],
            headers="keys",
        )
    )
