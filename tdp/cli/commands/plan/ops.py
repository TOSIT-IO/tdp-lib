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
    no_hosts_limit_option,
    preview_option,
    rolling_interval_option,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections


@click.command()
@click.argument("operation_names", nargs=-1, required=True)
@click.option(
    "-e",
    "--extra-vars",
    envvar="TDP_EXTRA_VARS",
    type=str,
    multiple=True,
    help="Extra vars for operations (forwarded to ansible as is). Can be used multiple times.",
)
@hosts_option(help="Hosts where operations are launched. Can be used multiple times.")
@collections_option
@database_dsn_option
@preview_option
@force_option
@no_hosts_limit_option(
    help="Works with --host and does not limit the operation to the specified hosts."
)
@rolling_interval_option
def ops(
    operation_names: tuple[str],
    extra_vars: tuple[str],
    hosts: tuple[str],
    collections: Collections,
    db_engine: Engine,
    preview: bool,
    force: bool,
    rolling_interval: Optional[int] = None,
    no_host_limit: Optional[tuple[str]] = None,
):
    """Run a list of operations."""

    from tdp.cli.utils import print_deployment
    from tdp.core.models import DeploymentModel
    from tdp.dao import Dao

    click.echo(
        f"Creating a deployment plan to run {len(operation_names)} operation(s)."
    )
    if no_host_limit and not hosts:
        click.BadOptionUsage("Cannot use `--no-host-limit` without --host argument.")
    deployment = DeploymentModel.from_operations(
        collections, operation_names, hosts, no_host_limit, extra_vars, rolling_interval
    )
    if preview:
        print_deployment(deployment)
        return
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
