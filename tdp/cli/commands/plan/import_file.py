# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import click

from tdp.cli.session import get_session
from tdp.cli.utils import collections, database_dsn, parse_file
from tdp.core.models.deployment_model import DeploymentModel
from tdp.queries import get_planned_deployment

if TYPE_CHECKING:
    from tdp.core.collections import Collections

logger = logging.getLogger(__name__)


@click.command("import")
@click.argument("file_name", nargs=1, required=True)
@collections
@database_dsn
def import_file(
    collections: Collections,
    database_dsn: str,
    file_name: str,
):
    """Import a deployment from a file."""
    with get_session(database_dsn, commit_on_exit=True) as session:
        planned_deployment = get_planned_deployment(session)
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
                deployment.id = planned_deployment.id
            session.merge(deployment)
            session.commit()
            click.echo("Deployment plan successfully imported.")
