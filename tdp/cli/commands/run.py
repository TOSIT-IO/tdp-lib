# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

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
from tdp.core.dag import Dag
from tdp.core.deployment import AnsibleExecutor, DeploymentPlan, DeploymentRunner
from tdp.core.models import DeploymentStateEnum
from tdp.core.variables import ClusterVariables


@click.command(short_help="Run single TDP operation")
@click.argument("operation_names", nargs=-1, required=True)
@dry
@collections
@database_dsn
@mock_deploy
@run_directory
@validate
@vars
def run(
    operation_names,
    dry,
    collections,
    database_dsn,
    mock_deploy,
    run_directory,
    validate,
    vars,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collections)

    operations = []
    for operation_name in operation_names:
        operation = dag.collections.operations.get(operation_name, None)
        if not operation:
            raise click.BadParameter(f"{operation_name} is not a valid operation")

        if operation.noop:
            raise click.BadParameter(
                f"{operation_name} is tagged as noop and thus"
                " cannot be executed in an unitary deployment"
            )
        operations.append(operation)

    run_directory = run_directory.absolute() if run_directory else None

    ansible_executor = AnsibleExecutor(
        run_directory=run_directory,
        dry=dry or mock_deploy,
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
        click.echo(f"Deploying {', '.join(map(lambda op: op.name, operations))}")
        deployment_plan = DeploymentPlan.from_operations(operations)
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
        if deployment_iterator.log.state != DeploymentStateEnum.SUCCESS:
            raise click.ClickException(
                (
                    "Deployment didn't finish with success: "
                    f"final state {deployment_iterator.log.state}"
                )
            )
