# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.session import get_session_class
from tdp.cli.utils import collections, database_dsn, vars
from tdp.core.dag import Dag
from tdp.core.deployment import DeploymentPlan

from tdp.cli.queries import get_planned_deployment_log


@click.command(short_help="Run single TDP operation.")
@click.argument("operation_names", nargs=-1, required=True)
@collections
@database_dsn
@vars
def run(
    operation_names,
    collections,
    database_dsn,
    vars,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist.")
    click.echo(
        f"Creating a deployment plan to run {len(operation_names)} operation(s)."
    )
    try:
        deployment_log = DeploymentPlan.from_operations(
            collections, operation_names
        ).deployment_log
    except Exception as e:
        raise click.ClickException(str(e)) from e

    session_class = get_session_class(database_dsn)
    with session_class() as session:
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log:
            deployment_log.id = planned_deployment_log.id
        session.merge(deployment_log)
        session.commit()
        click.echo("Deployment plan successfully created.")
