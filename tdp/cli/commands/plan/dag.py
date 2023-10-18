# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import click

from tdp.cli.queries import get_planned_deployment_log
from tdp.cli.session import get_session
from tdp.cli.utils import collections, database_dsn, preview, print_deployment
from tdp.core.dag import Dag
from tdp.core.models import DeploymentLog, FilterTypeEnum


def _validate_filtertype(ctx, param, value):
    if value is not None:
        return FilterTypeEnum[value]
    return value


@click.command(short_help="Deploy from the DAG.")
@click.option(
    "--sources",
    type=str,
    metavar="s1,s2,...",
    help="Nodes where the run start (separate with comma).",
)
@click.option(
    "--targets",
    type=str,
    metavar="t1,t2,...",
    help="Nodes where the run stop (separate with comma).",
)
@click.option("--filter", type=str, help="Match filter expression on dag result.")
@click.option(
    "--glob",
    "-g",
    "filter_type",
    callback=_validate_filtertype,
    flag_value=FilterTypeEnum.REGEX.name,
    help="Filter expression matched as a glob.",
)
@click.option(
    "--regex",
    "-r",
    "filter_type",
    callback=_validate_filtertype,
    flag_value=FilterTypeEnum.REGEX.name,
    help="Filter expression matched as a regex.",
)
@click.option(
    "--restart",
    is_flag=True,
    show_default=True,
    default=False,
    help="Replace 'start' operations by 'restart' operations.",
)
@click.option(
    "--stop",
    is_flag=True,
    show_default=True,
    default=False,
    help="Replace 'start' operations by 'stop' operations.",
)
@preview
@collections
@database_dsn
def dag(
    sources,
    targets,
    filter,
    filter_type,
    restart,
    preview,
    collections,
    database_dsn,
    stop: bool = False,
):
    if stop and restart:
        click.UsageError("Cannot use --restart and --stop at the same time.")
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
        raise click.BadParameter(f"{set_difference} are not valid nodes.")

    if sources:
        click.echo(f"Creating a deployment plan from: {sources}")
    elif targets:
        click.echo(f"Creating a deployment plan to: {targets}")
    else:
        click.echo("Creating a deployment plan for the whole DAG.")
    deployment_log = DeploymentLog.from_dag(
        dag,
        sources=sources,
        targets=targets,
        filter_expression=filter,
        filter_type=filter_type,
        restart=restart,
        stop=stop,
    )
    if preview:
        print_deployment(deployment_log)
        return
    with get_session(database_dsn, commit_on_exit=True) as session:
        planned_deployment_log = get_planned_deployment_log(session)
        if planned_deployment_log:
            deployment_log.id = planned_deployment_log.id
        session.merge(deployment_log)
    click.echo("Deployment plan successfully created.")
