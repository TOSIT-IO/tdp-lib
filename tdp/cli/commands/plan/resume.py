# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import click

from tdp.cli.session import get_session_class
from tdp.cli.utils import (
    collections,
    database_dsn,
    vars,
)
from tdp.core.dag import Dag
from tdp.core.deployment import DeploymentPlan
from tdp.cli.commands.utils import (
    execute_get_last_deployment_query,
    execute_get_deployment_query,
)


@click.command(short_help="Resume a TDP deployment")
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
        try:
            if id is None:
                deployment_log_to_resume = execute_get_last_deployment_query(session)
                click.echo(f"Plan from latest deployment.")
            else:
                deployment_log_to_resume = execute_get_deployment_query(session, id)
                click.echo(f"Plan from deployment #{id}.")

            deployment_plan = DeploymentPlan.from_failed_deployment(
                dag, deployment_log_to_resume
            )
        except Exception as e:
            raise click.ClickException(str(e)) from e

        deployment_log = deployment_plan.getDeploymentLog()
        session.add(deployment_log)
        session.commit()
