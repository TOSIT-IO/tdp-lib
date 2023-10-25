# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.queries import get_planned_deployment
from tdp.cli.session import get_session
from tdp.cli.utils import collections, database_dsn, preview, print_deployment
from tdp.core.dag import Dag
from tdp.core.models import DeploymentModel, FilterTypeEnum

if TYPE_CHECKING:
    from tdp.core.collections import Collections


def _validate_filtertype(
    ctx: click.Context, param: click.Parameter, value: FilterTypeEnum
):
    if value is not None:
        return FilterTypeEnum[value]
    return value


# TODO: remove glob filter type (see https://github.com/TOSIT-IO/tdp-lib/issues/478)
@click.command()
@click.option(
    "--source",
    "sources",
    type=str,
    multiple=True,
    help="Nodes where the run start. Can be used multiple times.",
)
@click.option(
    "--target",
    "targets",
    type=str,
    multiple=True,
    help="Nodes where the run stop. Can be used multiple times.",
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
    help="Replace 'start' operations by 'restart' operations.",
)
@click.option(
    "--reverse",
    is_flag=True,
    help="Reverse the final list of operations.",
)
@click.option(
    "--stop",
    is_flag=True,
    help="Replace 'start' operations by 'stop' operations. This option should be used with --reversed.",
)
@preview
@collections
@database_dsn
def dag(
    sources: tuple[str],
    targets: tuple[str],
    restart: bool,
    preview: bool,
    collections: Collections,
    database_dsn: str,
    reverse: bool,
    stop: bool,
    filter: Optional[str] = None,
    filter_type: Optional[FilterTypeEnum] = None,
):
    """Deploy from the DAG."""
    if stop and restart:
        click.UsageError("Cannot use --restart and --stop at the same time.")
    dag = Dag(collections)
    set_nodes = set()
    if sources:
        set_nodes.update(sources)
    if targets:
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
    deployment = DeploymentModel.from_dag(
        dag,
        sources=sources,
        targets=targets,
        filter_expression=filter,
        filter_type=filter_type,
        restart=restart,
        reverse=reverse,
        stop=stop,
    )
    if preview:
        print_deployment(deployment)
        return
    with get_session(database_dsn, commit_on_exit=True) as session:
        planned_deployment = get_planned_deployment(session)
        if planned_deployment:
            deployment.id = planned_deployment.id
        session.merge(deployment)
    click.echo("Deployment plan successfully created.")
