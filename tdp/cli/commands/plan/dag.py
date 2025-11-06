# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import (
    collections_option,
    database_dsn_option,
    force_option,
    hosts_option,
    preview_option,
    rolling_interval_option,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections


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
    "is_regex",
    is_flag=True,
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
@hosts_option(help="Hosts where operations are launched. Can be used multiple times.")
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
    db_engine: Engine,
    reverse: bool,
    stop: bool,
    filter: Optional[str] = None,
    is_regex: bool = False,
    rolling_interval: Optional[int] = None,
    hosts: Optional[tuple[str]] = None,
):
    """Deploy from the DAG."""

    from tdp.cli.utils import print_deployment
    from tdp.core.constants import NO_HOST_LIMIT_OPERATION_SUFFIX
    from tdp.core.dag import Dag
    from tdp.core.models import DeploymentModel
    from tdp.core.models.enums import FilterTypeEnum
    from tdp.dao import Dao

    filter_type = None
    if filter:
        filter_type = FilterTypeEnum.REGEX if is_regex else FilterTypeEnum.GLOB
    if stop and restart:
        click.UsageError("Cannot use `--restart` and `--stop` at the same time.")

    dag = Dag(collections)

    # Check that sources and targets are valid DAG nodes
    set_nodes: set[str] = set()
    if sources:
        set_nodes.update(sources)
    if targets:
        set_nodes.update(targets)
    if set_nodes:
        dag_nodes = [str(op.name) for op in dag.operations]
        set_difference = set_nodes.difference(dag_nodes)
        if set_difference:
            raise click.BadParameter(
                f"{set_difference} are not valid DAG operation(s)."
            )

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
        host_names=hosts,
    )
    if preview:
        print_deployment(deployment)
        return
    if hosts and any(
        operation_suffix
        for operation_suffix in NO_HOST_LIMIT_OPERATION_SUFFIX
        for operation_suffix in deployment.operations
    ):
        click.echo(
            f"WARNING: {', '.join([operation.operation for operation in deployment.operations if any(operation_suffix in operation.operation for operation_suffix in NO_HOST_LIMIT_OPERATION_SUFFIX)])} can not be limited to certain hosts"
        )

    with Dao(db_engine, commit_on_exit=True) as dao:
        planned_deployment = dao.get_planned_deployment()
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
