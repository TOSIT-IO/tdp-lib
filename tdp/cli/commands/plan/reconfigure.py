# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import get_planned_deployment_log, get_stale_components
from tdp.cli.session import get_session
from tdp.cli.utils import (
    collections,
    database_dsn,
    preview,
    print_deployment,
    rolling_interval,
)
from tdp.core.models import DeploymentLog


@click.command(short_help="Restart required TDP services.")
@collections
@database_dsn
@preview
@rolling_interval
def reconfigure(
    collections,
    database_dsn,
    preview,
    rolling_interval,
):
    click.echo("Creating a deployment plan to reconfigure services.")
    with get_session(database_dsn, commit_on_exit=True) as session:
        stale_components = get_stale_components(session)
        deployment_log = DeploymentLog.from_stale_components(
            collections, stale_components, rolling_interval
        )
        if preview:
            print_deployment(deployment_log)
            return
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log:
            deployment_log.id = planned_deployment_log.id
        session.merge(deployment_log)
    click.echo("Deployment plan successfully created.")
