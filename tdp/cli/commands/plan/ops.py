# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click
from sqlalchemy import Engine

from tdp.cli.params import collections_option, database_dsn_option, hosts_option
from tdp.cli.params.plan import force_option, preview_option, rolling_interval_option
from tdp.cli.utils import (
    print_deployment,
)
from tdp.core.entities.deployment_entity import transform_to_deployment_entity
from tdp.core.models import DeploymentModel
from tdp.dao import Dao

if TYPE_CHECKING:
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
):
    """Run a list of operations."""
    click.echo(
        f"Creating a deployment plan to run {len(operation_names)} operation(s)."
    )
    deployment = DeploymentModel.from_operations(
        collections, operation_names, hosts, extra_vars, rolling_interval
    )
    if preview:
        print_deployment(transform_to_deployment_entity(deployment))
        return
    with Dao(db_engine, commit_on_exit=True) as dao:
        planned_deployment = dao.get_planned_deployment_dao()
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
