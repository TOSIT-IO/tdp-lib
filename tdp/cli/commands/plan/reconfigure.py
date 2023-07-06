# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import get_latest_success_component_version_log
from tdp.cli.session import get_session_class
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    validate,
    vars,
)
from tdp.core.dag import Dag
from tdp.core.deployment import (
    DeploymentPlan,
    EmptyDeploymentPlanError,
    NothingToRestartError,
)
from tdp.core.variables import ClusterVariables

from tdp.cli.queries import get_planned_deployment_log


@click.command(short_help="Restart required TDP services.")
@collections
@database_dsn
@validate
@vars
def reconfigure(
    collections,
    database_dsn,
    validate,
    vars,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist.")
    dag = Dag(collections)

    session_class = get_session_class(database_dsn)
    with session_class() as session:
        latest_success_component_version_log = (
            get_latest_success_component_version_log(session)
        )
        latest_success_component_version = map(
            lambda result: result[1:], latest_success_component_version_log
        )
        cluster_variables = ClusterVariables.get_cluster_variables(
            collections, vars, validate=validate
        )
        check_services_cleanliness(cluster_variables)

        try:
            click.echo(f"Creating a deployment plan to reconfigure services.")
            deployment_log = DeploymentPlan.from_reconfigure(
                dag, cluster_variables, latest_success_component_version
            ).deployment_log
        except NothingToRestartError:
            click.echo("Nothing needs to be restarted.")
            return
        except EmptyDeploymentPlanError:
            raise click.ClickException(
                f"Component(s) don't have any operation associated to restart (excluding noop). Nothing to restart."
            )
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log:
            deployment_log.id = planned_deployment_log.id
        session.merge(deployment_log)
        session.commit()
        click.echo("Deployment plan successfully created.")
