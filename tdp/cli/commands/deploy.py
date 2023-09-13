# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import (
    get_planned_deployment_log,
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


@click.command(short_help="Execute a TDP deployment plan.")
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
    mock_deploy,
    run_directory,
    validate,
    vars,
):
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections, vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with get_session(database_dsn, commit_on_exit=True) as session:
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log is None:
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
        ).run(planned_deployment_log)

        if dry:
            for _ in deployment_iterator:
                pass
            return

        session.commit()  # Update deployment log status to RUNNING
        for cluster_status_logs in deployment_iterator:
            if cluster_status_logs and any(cluster_status_logs):
                session.add_all(cluster_status_logs)
                session.commit()

        if deployment_iterator.deployment_log.status != DeploymentStateEnum.SUCCESS:
            raise click.ClickException("Deployment failed.")
        else:
            click.echo("Deployment finished with success.")
