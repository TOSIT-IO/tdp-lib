# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import collections_option, database_dsn_option
from tdp.cli.params.plan import force_option, preview_option, rolling_interval_option
from tdp.cli.queries import get_planned_deployment
from tdp.cli.utils import (
    print_deployment,
)
from tdp.core.models import DeploymentModel
from tdp.dao import Dao

if TYPE_CHECKING:
    from tdp.core.collections import Collections


@click.command()
@collections_option
@database_dsn_option
@preview_option
@force_option
@rolling_interval_option
def reconfigure(
    collections: Collections,
    database_dsn: str,
    preview: bool,
    force: bool,
    rolling_interval: Optional[int] = None,
):
    """Reconfigure required TDP services."""
    click.echo("Creating a deployment plan to reconfigure services.")
    with Dao(database_dsn, commit_on_exit=True) as dao:
        deployment = DeploymentModel.from_stale_hosted_entities(
            collections=collections,
            stale_hosted_entity_statuses=dao.get_stale_hosted_entity_statuses(),
            rolling_interval=rolling_interval,
        )
        if preview:
            print_deployment(deployment)
            return
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
