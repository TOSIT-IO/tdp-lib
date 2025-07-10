# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import click

from tdp.cli.params import collections_option, database_dsn_option
from tdp.cli.params.plan import force_option

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections

logger = logging.getLogger(__name__)


@click.command("import")
@click.argument("file_name", nargs=1, required=True)
@force_option
@collections_option
@database_dsn_option
def import_file(
    collections: Collections,
    db_engine: Engine,
    file_name: str,
    force: bool,
):
    """Import a deployment from a file."""

    from tdp.cli.utils import parse_file
    from tdp.core.models.deployment_model import DeploymentModel
    from tdp.dao import Dao

    with Dao(db_engine, commit_on_exit=True) as dao:
        planned_deployment = dao.get_planned_deployment()
        with open(file_name) as file:
            # Remove empty elements and comments
            # and get the operations, hosts and extra vars in a list
            new_operations_hosts_vars = parse_file(file)
            if not new_operations_hosts_vars:
                raise click.ClickException("Plan must not be empty.")
            deployment = DeploymentModel.from_operations_hosts_vars(
                collections, new_operations_hosts_vars
            )
            # if a planned deployment is present, update it instead of creating it
            if planned_deployment:
                if force or click.confirm(
                    "A deployment plan already exists, do you want to override it?"
                ):
                    deployment.id = planned_deployment.id
                else:
                    click.echo("No new deployment plan has been created.")
                    return
            dao.session.merge(deployment)
            dao.session.commit()
            click.echo("Deployment plan successfully imported.")
