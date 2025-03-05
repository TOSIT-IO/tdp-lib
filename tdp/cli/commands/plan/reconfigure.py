# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click
from sqlalchemy import Engine

from tdp.cli.params.collections_option import collections_option
from tdp.cli.params.database_dsn_option import database_dsn_option
from tdp.cli.params.plan.force_option import force_option
from tdp.cli.params.plan.preview_option import preview_option
from tdp.cli.params.plan.rolling_interval_option import rolling_interval_option
from tdp.cli.utils import print_deployment
from tdp.core.models.deployment_model import DeploymentModel
from tdp.dao import Dao

if TYPE_CHECKING:
    from tdp.core.collections.collections import Collections


@click.command()
@collections_option
@database_dsn_option
@preview_option
@force_option
@rolling_interval_option
def reconfigure(
    collections: Collections,
    db_engine: Engine,
    preview: bool,
    force: bool,
    rolling_interval: Optional[int] = None,
):
    """Reconfigure required TDP services."""
    click.echo("Creating a deployment plan to reconfigure services.")
    with Dao(db_engine, commit_on_exit=True) as dao:
        deployment = DeploymentModel.from_stale_hosted_entities(
            collections=collections,
            stale_hosted_entity_statuses=dao.get_hosted_entity_statuses(
                filter_stale=True
            ),
            rolling_interval=rolling_interval,
        )
        if preview:
            print_deployment(deployment)
            return
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
