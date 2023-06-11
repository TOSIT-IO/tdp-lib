# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import click

from tdp.cli.queries import get_deployment, get_last_deployment
from tdp.cli.session import get_session_class
from tdp.cli.utils import (
    collections,
    database_dsn,
    vars,
)
from tdp.core.dag import Dag
from tdp.core.deployment import DeploymentPlan
from tdp.core.models import DeploymentStateEnum, OperationStateEnum, OperationLog


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
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collections)

    session_class = get_session_class(database_dsn)
    with session_class() as session:
        if id is None:
            #todo: ne pas passer la session class en arg, la fonction doit renvoyer un objet select
            deployment_log_to_resume = get_last_deployment(session_class)
            click.echo(f"Resuming latest deployment")
        else:
            deployment_log_to_resume = get_deployment(session_class, id)
            click.echo(f"Resuming deployment #{id}")
        try:
            deployment_plan = DeploymentPlan.from_failed_deployment(
                dag, deployment_log_to_resume
            )
        except Exception as e:
            raise click.ClickException(str(e)) from e

        deployment_log = deployment_plan.getDeploymentLog()
        session.add(deployment_log)
        session.commit()
