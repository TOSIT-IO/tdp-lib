# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import collections_option, database_dsn_option
from tdp.cli.params.plan import preview_option

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections


@click.command()
@click.argument("id", required=False)
@collections_option
@database_dsn_option
@preview_option
def resume(
    collections: Collections,
    db_engine: Engine,
    preview: bool,
    id: Optional[int] = None,
):
    """Resume a failed deployment."""

    from tdp.cli.utils import print_deployment
    from tdp.core.models import DeploymentModel
    from tdp.dao import Dao

    with Dao(db_engine, commit_on_exit=True) as dao:
        if id is None:
            deployment_to_resume = dao.get_last_deployment()
            if not deployment_to_resume:
                raise click.ClickException("No deployment found.")
            click.echo("Creating a deployment plan to resume latest deployment.")
        else:
            deployment_to_resume = dao.get_deployment(id)
            if not deployment_to_resume:
                raise click.ClickException(f"Deployment {id} not found.")
            click.echo(f"Creating a deployment plan to resume deployment {id}.")
        deployment = DeploymentModel.from_failed_deployment(
            collections, deployment_to_resume
        )
        if preview:
            print_deployment(deployment)
            return
        planned_deployment = dao.get_planned_deployment()
        if planned_deployment:
            deployment.id = planned_deployment.id
        dao.session.merge(deployment)
    click.echo("Deployment plan successfully created.")
