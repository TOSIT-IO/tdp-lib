# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import (
    get_stale_components,
    get_planned_deployment_log,
)
from tdp.cli.session import get_session_class
from tdp.cli.utils import (
    collections,
    database_dsn,
)
from tdp.core.models import DeploymentLog


@click.command(short_help="Restart required TDP services.")
@collections
@database_dsn
def reconfigure(
    collections,
    database_dsn,
):
    session_class = get_session_class(database_dsn)
    with session_class() as session:
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
        session.commit()
        click.echo("Deployment plan successfully created.")
