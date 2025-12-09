# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import (
    collections_option,
    database_dsn_option,
    force_option,
    preview_option,
    rolling_interval_option,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections


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

    from tdp.cli.utils import print_deployment, validate_plan_creation
    from tdp.core.models import DeploymentModel
    from tdp.dao import Dao

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
        if last_deployment := dao.get_last_deployment():
            validate_plan_creation(last_deployment.state, force)
            deployment.id = last_deployment.id
        dao.session.merge(deployment)
    click.echo("Deployment plan successfully created.")
