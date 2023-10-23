# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import (
    get_planned_deployment,
    get_sch_status,
)
from tdp.cli.session import get_session
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    dry,
    mock_deploy,
    run_directory,
    validate,
    vars,
)
from tdp.core.cluster_status import ClusterStatus
from tdp.core.deployment import DeploymentRunner, Executor
from tdp.core.models import DeploymentStateEnum
from tdp.core.variables import ClusterVariables


@click.command()
@click.option(
    "--force-stale-update",
    "--fsu",
    "force_stale_update",
    is_flag=True,
    help="Force stale status update.",
)
@dry
@collections
@database_dsn
@mock_deploy
@run_directory
@validate
@vars
def deploy(
    dry,
    collections,
    database_dsn,
    force_stale_update: bool,
    mock_deploy,
    run_directory,
    validate,
    vars,
):
    """Execute a planned deployment."""
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections, vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with get_session(database_dsn, commit_on_exit=True) as session:
        planned_deployment = get_planned_deployment(session)
        if planned_deployment is None:
            raise click.ClickException(
                "No planned deployment found, please run `tdp plan` first."
            )

        deployment_iterator = DeploymentRunner(
            collections=collections,
            executor=Executor(
                run_directory=run_directory.absolute() if run_directory else None,
                dry=dry or mock_deploy,
            ),
            cluster_variables=cluster_variables,
            cluster_status=ClusterStatus.from_sch_status_rows(get_sch_status(session)),
        ).run(planned_deployment, force_stale_update=force_stale_update)

        if dry:
            for _ in deployment_iterator:
                pass
            return

        session.commit()  # Update deployment status to RUNNING
        for cluster_status_logs in deployment_iterator:
            if cluster_status_logs and any(cluster_status_logs):
                session.add_all(cluster_status_logs)
                session.commit()

        if deployment_iterator.deployment.status != DeploymentStateEnum.SUCCESS:
            raise click.ClickException("Deployment failed.")
        else:
            click.echo("Deployment finished with success.")
