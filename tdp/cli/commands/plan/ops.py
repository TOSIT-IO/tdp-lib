# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.queries import get_planned_deployment
from tdp.cli.session import get_session
from tdp.cli.utils import (
    collections,
    database_dsn,
    hosts,
    preview,
    print_deployment,
    rolling_interval,
)
from tdp.core.models.deployment_model import DeploymentModel

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
@hosts(help="Hosts where operations are launched. Can be used multiple times.")
@collections
@database_dsn
@preview
@rolling_interval
def ops(
    operation_names: tuple[str],
    extra_vars: tuple[str],
    hosts: tuple[str],
    collections: Collections,
    database_dsn: str,
    preview: bool,
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
        print_deployment(deployment)
        return
    with get_session(database_dsn, commit_on_exit=True) as session:
        planned_deployment = get_planned_deployment(session)
        if planned_deployment:
            deployment.id = planned_deployment.id
        session.merge(deployment)
    click.echo("Deployment plan successfully created.")
