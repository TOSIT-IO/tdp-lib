# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import collections_option, database_dsn_option
from tdp.cli.params.plan import force_option, preview_option, rolling_interval_option
from tdp.cli.queries import get_planned_deployment
from tdp.cli.utils import print_deployment
from tdp.core.dag import Dag
from tdp.core.models import DeploymentModel
from tdp.core.models.enums import FilterTypeEnum
from tdp.dao import Dao

if TYPE_CHECKING:
    from tdp.core.collections import Collections


def _validate_filtertype(
    ctx: click.Context, param: click.Parameter, value: FilterTypeEnum
):
    if value is not None:
        return FilterTypeEnum[value]
    return value


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
    help="Replace 'start' operations by 'stop' operations. This option should be used with `--reverse`.",
)
@rolling_interval_option
@preview_option
@force_option
@collections_option
@database_dsn_option
def dag(
    sources: tuple[str],
    targets: tuple[str],
    restart: bool,
    preview: bool,
    force: bool,
    collections: Collections,
    database_dsn: str,
    reverse: bool,
    stop: bool,
    filter: Optional[str] = None,
    filter_type: Optional[FilterTypeEnum] = None,
    rolling_interval: Optional[int] = None,
):
    """Deploy from the DAG."""
    if stop and restart:
        click.UsageError("Cannot use `--restart` and `--stop` at the same time.")
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
        rolling_interval=rolling_interval,
    )
    if preview:
        print_deployment(deployment)
        return
    with Dao(database_dsn, commit_on_exit=True) as dao:
        planned_deployment = get_planned_deployment(dao.session)
        if planned_deployment:
            if force or click.confirm(
                "A deployment plan already exists, do you want to override it?"
            ):
                deployment.id = planned_deployment.id
            else:
                click.echo("No new deployment plan has been created.")
                return
        dao.session.merge(deployment)
    click.echo("Deployment plan successfully created.")
