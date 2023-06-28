# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import get_planned_deployment_log
from tdp.cli.session import get_session_class
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
from tdp.core.deployment import Executor, DeploymentPlan, DeploymentRunner
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
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist.")

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections, vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    deployment_runner = DeploymentRunner(
        collections,
        Executor(
            run_directory=run_directory.absolute() if run_directory else None,
            dry=dry or mock_deploy,
        ),
        cluster_variables,
    )

    session_class = get_session_class(database_dsn)
    with session_class() as session:
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log is None:
            raise click.ClickException(
                "No planned deployment found, please run `tdp plan` first."
            )
        deployment_plan = DeploymentPlan.from_deployment_log(
            collections, planned_deployment_log
        )
        deployment_iterator = deployment_runner.run(deployment_plan)
        if dry:
            for _ in deployment_iterator:
                pass
        else:
            # Update deployment log to RUNNING
            session.merge(deployment_iterator.deployment_log)
            session.commit()
            for operation_log, service_component_log in deployment_iterator:
                if operation_log is not None:
                    session.merge(operation_log)
                if service_component_log is not None:
                    session.add(service_component_log)
                session.commit()
            # Update deployment log to SUCCESS or FAILURE
            session.merge(deployment_iterator.deployment_log)
            session.commit()
        if deployment_iterator.deployment_log.state != DeploymentStateEnum.SUCCESS:
            raise click.ClickException(
                (
                    "Deployment didn't finish with success: "
                    f"final state {deployment_iterator.deployment_log.state}"
                )
            )
        else:
            click.echo("Deployment finished with success.")
