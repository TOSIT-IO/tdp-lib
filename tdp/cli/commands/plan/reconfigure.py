# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import (
    get_latest_success_component_version_log,
    get_planned_deployment_log,
)
from tdp.cli.session import get_session_class
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    validate,
    vars,
)
from tdp.core.dag import Dag
from tdp.core.models import DeploymentLog
from tdp.core.variables import ClusterVariables


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
        latest_success_components_version_log = (
            get_latest_success_component_version_log(session)
        )
        cluster_variables = ClusterVariables.get_cluster_variables(
            collections, vars, validate=validate
        )
        check_services_cleanliness(cluster_variables)

        try:
            click.echo(f"Creating a deployment plan to reconfigure services.")
            deployment_log = DeploymentLog.from_reconfigure(
                dag, cluster_variables, latest_success_components_version_log
            )
        except Exception as e:
            raise click.ClickException(f"Failed to create deployment plan: {e}") from e
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log:
            deployment_log.id = planned_deployment_log.id
        session.merge(deployment_log)
        session.commit()
        click.echo("Deployment plan successfully created.")
