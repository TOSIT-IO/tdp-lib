# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import get_planned_deployment_log, get_stale_components
from tdp.cli.session import get_session
from tdp.cli.utils import collections, database_dsn
from tdp.core.models import DeploymentLog


@click.command(short_help="Restart required TDP services.")
@collections
@database_dsn
def reconfigure(
    collections,
    database_dsn,
):
    with get_session(database_dsn, commit_on_exit=True) as session:
        try:
            click.echo(f"Creating a deployment plan to reconfigure services.")
            stale_components = get_stale_components(session)
            deployment_log = DeploymentLog.from_stale_components(
                collections, stale_components
            )
        except Exception as e:
            raise click.ClickException(str(e)) from e
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log:
            deployment_log.id = planned_deployment_log.id
        session.merge(deployment_log)
        click.echo("Deployment plan successfully created.")
