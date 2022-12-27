# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import click

from tdp.cli.session import get_session_class
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    dry,
    run_directory,
    vars,
)
from tdp.core.dag import Dag
from tdp.core.deployment import AnsibleExecutor, DeploymentPlan, DeploymentRunner
from tdp.core.models import FilterTypeEnum, StateEnum
from tdp.core.variables import ClusterVariables


def validate_filtertype(ctx, param, value):
    if value is not None:
        return FilterTypeEnum[value]
    return value


@click.command(short_help="Deploy TDP")
@click.option(
    "--sources",
    type=str,
    metavar="s1,s2,...",
    help="Nodes where the run start (separate with comma)",
)
@click.option(
    "--targets",
    type=str,
    metavar="t1,t2,...",
    help="Nodes where the run stop (separate with comma)",
)
@click.option("--filter", type=str, help="Match filter expression on dag result")
@click.option(
    "--glob",
    "-g",
    "filter_type",
    callback=validate_filtertype,
    flag_value=FilterTypeEnum.REGEX.name,
    help="Filter expression matched as a glob",
)
@click.option(
    "--regex",
    "-r",
    "filter_type",
    callback=validate_filtertype,
    flag_value=FilterTypeEnum.REGEX.name,
    help="Filter expression matched as a regex",
)
@click.option(
    "--restart",
    is_flag=True,
    show_default=True,
    default=False,
    help="Whether start operations should be replaced by restart operations.",
)
@dry
@collections
@database_dsn
@run_directory
@vars
def deploy(
    sources,
    targets,
    filter,
    filter_type,
    restart,
    dry,
    collections,
    database_dsn,
    run_directory,
    vars,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collections)
    set_nodes = set()
    if sources:
        sources = sources.split(",")
        set_nodes.update(sources)
    if targets:
        targets = targets.split(",")
        set_nodes.update(targets)
    set_difference = set_nodes.difference(dag.operations)
    if set_difference:
        raise click.BadParameter(f"{set_difference} are not valid nodes")
    run_directory = run_directory.absolute() if run_directory else None

    ansible_executor = AnsibleExecutor(
        run_directory=run_directory,
        dry=dry,
    )
    session_class = get_session_class(database_dsn)
    with session_class() as session:
        cluster_variables = ClusterVariables.get_cluster_variables(collections, vars)
        check_services_cleanliness(cluster_variables)

        deployment_runner = DeploymentRunner(
            collections, ansible_executor, cluster_variables
        )
        if sources:
            click.echo(f"Deploying from {sources}")
        elif targets:
            click.echo(f"Deploying to {targets}")
        else:
            click.echo(f"Deploying TDP")
        try:
            deployment_plan = DeploymentPlan.from_dag(
                dag,
                sources=sources,
                targets=targets,
                filter_expression=filter,
                filter_type=filter_type,
                restart=restart,
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
