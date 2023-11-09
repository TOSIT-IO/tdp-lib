# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.queries import (
    get_deployment,
    get_last_deployment,
    get_planned_deployment,
)
from tdp.cli.session import get_session
from tdp.cli.utils import collections, database_dsn, preview, print_deployment
from tdp.core.models import DeploymentModel

if TYPE_CHECKING:
    from tdp.core.collections import Collections


@click.command()
@click.argument("id", required=False)
@collections
@database_dsn
@preview
def resume(
    collections: Collections,
    database_dsn: str,
    preview: bool,
    id: Optional[int] = None,
):
    """Resume a failed deployment."""
    with get_session(database_dsn, commit_on_exit=True) as session:
        if id is None:
            deployment_to_resume = get_last_deployment(session)
            click.echo("Creating a deployment plan to resume latest deployment.")
        else:
            deployment_to_resume = get_deployment(session, id)
            click.echo(f"Creating a deployment plan to resume deployment {id}.")
        deployment = DeploymentModel.from_failed_deployment(
            collections, deployment_to_resume
        )
        if preview:
            print_deployment(deployment)
            return
        planned_deployment = get_planned_deployment(session)
        if planned_deployment:
            deployment.id = planned_deployment.id
        session.merge(deployment)
    click.echo("Deployment plan successfully created.")
