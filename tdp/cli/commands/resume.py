# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import click

from tdp.cli.queries import get_deployment, get_last_deployment
from tdp.cli.session import get_session_class
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    dry,
    run_directory,
    validate,
    vars,
)
from tdp.core.dag import Dag
from tdp.core.deployment import AnsibleExecutor, DeploymentPlan, DeploymentRunner
from tdp.core.models.state_enum import StateEnum
from tdp.core.variables import ClusterVariables


@click.command(short_help="Resume a TDP deployment")
@click.argument("id", required=False)
@dry
@collections
@database_dsn
@run_directory
@validate
@vars
def resume(
    id,
    dry,
    collections,
    database_dsn,
    run_directory,
    validate,
    vars,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collections)
    run_directory = run_directory.absolute() if run_directory else None

    ansible_executor = AnsibleExecutor(
        run_directory=run_directory,
        dry=dry,
    )
    session_class = get_session_class(database_dsn)
    with session_class() as session:
        cluster_variables = ClusterVariables.get_cluster_variables(
            collections, vars, validate=validate
        )
        check_services_cleanliness(cluster_variables)

        deployment_runner = DeploymentRunner(
            collections, ansible_executor, cluster_variables
        )
        if id is None:
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
        deployment_iterator = deployment_runner.run(deployment_plan)
        if dry:
            for _ in deployment_iterator:
                pass
        else:
            session.add(deployment_iterator.log)
            # insert pending deployment log
            session.commit()
            for operation_log, service_component_log in deployment_iterator:
                if operation_log is not None:
                    session.add(operation_log)
                if service_component_log is not None:
                    session.add(service_component_log)
                session.commit()
            # notify sqlalchemy deployment log has been updated
            session.merge(deployment_iterator.log)
            session.commit()
        if deployment_iterator.log.state != StateEnum.SUCCESS:
            raise click.ClickException(
                (
                    "Deployment didn't finish with success: "
                    f"final state {deployment_iterator.log.state}"
                )
            )
