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
from tdp.core.models import DeploymentStateEnum, FilterTypeEnum, OperationLog


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
@collections
@database_dsn
@vars
def dag(
    sources,
    targets,
    filter,
    filter_type,
    restart,
    collections,
    database_dsn,
    vars,
):
    # Check parameters
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

    # Generate DeploymentLog from a DeploymentPlan
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
    deployment_log = deployment_plan.getDeploymentLog()

    # Persist the deployment_log in the database
    session_class = get_session_class(database_dsn)
    with session_class() as session:
        session.add(deployment_log)
        session.commit()
