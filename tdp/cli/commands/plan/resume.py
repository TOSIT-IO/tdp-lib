# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import click

from tdp.cli.queries import (
    get_deployment,
    get_last_deployment,
    get_planned_deployment_log,
)
from tdp.cli.session import get_session
from tdp.cli.utils import collections, database_dsn, vars
from tdp.core.dag import Dag
from tdp.core.models import DeploymentLog


@click.command(short_help="Resume a failed deployment.")
@click.argument("id", required=False)
@collections
@database_dsn
@vars
def resume(
    id,
    collections,
    database_dsn,
    vars,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist.")
    dag = Dag(collections)

    with get_session(database_dsn, commit_on_exit=True) as session:
        if id is None:
            deployment_log_to_resume = get_last_deployment(session)
            click.echo(f"Creating a deployment plan to resume latest deployment.")
        else:
            deployment_log_to_resume = get_deployment(session, id)
            click.echo(f"Creating a deployment plan to resume deployment #{id}.")
        deployment_log = DeploymentLog.from_failed_deployment(
            collections, deployment_log_to_resume
        )
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log:
            deployment_log.id = planned_deployment_log.id
        session.merge(deployment_log)
        click.echo("Deployment plan successfully created.")
