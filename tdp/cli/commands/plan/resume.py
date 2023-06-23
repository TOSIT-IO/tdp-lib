# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import click

from tdp.cli.queries import get_deployment, get_last_deployment
from tdp.cli.session import get_session_class
from tdp.cli.utils import collections, database_dsn, vars
from tdp.core.dag import Dag
from tdp.core.deployment import DeploymentPlan

from .utils import get_planned_deployment_log


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

    session_class = get_session_class(database_dsn)
    with session_class() as session:
        if id is None:
            deployment_log_to_resume = get_last_deployment(session_class)
            click.echo(f"Creating a deployment plan to resume latest deployment.")
        else:
            deployment_log_to_resume = get_deployment(session_class, id)
            click.echo(f"Creating a deployment plan to resume deployment #{id}.")
        try:
            deployment_log = DeploymentPlan.from_failed_deployment(
                dag, deployment_log_to_resume
            ).deployment_log
        except Exception as e:
            raise click.ClickException(str(e)) from e
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log:
            deployment_log.id = planned_deployment_log.id
        session.merge(deployment_log)
        session.commit()
        click.echo("Deployment plan successfully created.")
