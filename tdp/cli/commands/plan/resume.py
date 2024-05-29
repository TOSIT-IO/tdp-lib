# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click
from sqlalchemy import Engine

from tdp.cli.params import collections_option, database_dsn_option
from tdp.cli.params.plan import preview_option
from tdp.cli.utils import print_deployment
from tdp.core.entities.deployment_entity import transform_to_deployment_entity
from tdp.core.models import DeploymentModel
from tdp.dao import Dao

if TYPE_CHECKING:
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
    with Dao(db_engine, commit_on_exit=True) as dao:
        if id is None:
            deployment_to_resume = dao.get_last_deployment_dao()
            click.echo("Creating a deployment plan to resume latest deployment.")
        else:
            deployment_to_resume = dao.get_deployment_dao(id)
            click.echo(f"Creating a deployment plan to resume deployment {id}.")
        deployment = DeploymentModel.from_failed_deployment(
            collections, deployment_to_resume.transform_to_deployment_model()
        )
        if preview:
            print_deployment(transform_to_deployment_entity(deployment))
            return
        planned_deployment = dao.get_planned_deployment_dao()
        if planned_deployment:
            deployment.id = planned_deployment.id
        dao.session.merge(deployment)
    click.echo("Deployment plan successfully created.")
