# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import collections_option, database_dsn_option
from tdp.cli.params.plan import preview_option, rolling_interval_option
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
@rolling_interval_option
def reconfigure(
    collections: Collections,
    database_dsn: str,
    preview: bool,
    rolling_interval: Optional[int] = None,
):
    """Reconfigure required TDP services."""
    click.echo("Creating a deployment plan to reconfigure services.")
    with Dao(database_dsn, commit_on_exit=True) as dao:
        deployment = DeploymentModel.from_stale_components(
            collections=collections,
            cluster_status=dao.get_sch_status(),
            rolling_interval=rolling_interval,
        )
        if preview:
            print_deployment(deployment)
            return
        planned_deployment = get_planned_deployment(dao.session)
        if planned_deployment:
            deployment.id = planned_deployment.id
        dao.session.merge(deployment)
    click.echo("Deployment plan successfully created.")
