# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import (
    collections_option,
    component_argument_option,
    database_dsn_option,
    service_argument_option,
    validate_option,
    vars_option,
)
from tdp.cli.utils import validate_service_component

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections


logger = logging.getLogger(__name__)


@click.command()
@service_argument_option(required=True)
@component_argument_option
@click.option(
    "--commit_message",
    "-c",
    type=str,
    default="updated from `tdp vars edit` command",
    help="Validation message for the service repository.",
)
@collections_option
@database_dsn_option
@validate_option
@vars_option
def edit(
    commit_message: str,
    collections: Collections,
    db_engine: Engine,
    validate: bool,
    vars: Path,
    service: str,
    component: Optional[str] = None,
):
    """Edit a variables file.

    SERVICE_NAME: Name of the service to edit variables for.
    SERVICE_COMPONENT_PARAMETER: Name or path of the component to edit variables for. If not specified, edit the service variables file.

    \b
    Examples:
        tdp vars edit hdfs
        tdp vars edit hdfs datanode --commit_message "updated datanode variables"
        tdp vars edit hdfs hdfs_datanode.yml
    """

    from tdp.core.constants import YML_EXTENSION
    from tdp.core.entities.entity_name import create_entity_name
    from tdp.core.exceptions import ServiceVariablesNotInitializedErrorList
    from tdp.core.repository.repository import EmptyCommit
    from tdp.core.variables import ClusterVariables
    from tdp.core.variables.schema.exceptions import InvalidSchemaError
    from tdp.dao import Dao

    # Validate service and component arguments
    if component and component.endswith(YML_EXTENSION):
        component = component[: -len(YML_EXTENSION)]
    if component == service or not component:
        component = None
    validate_service_component(service, component, collections=collections)

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections, vars, validate=validate
    )
    service_variables = cluster_variables[service]
    repo = service_variables.repository

    # Check if service is clean
    if not service_variables.clean:
        raise click.ClickException(
            f"Error service '{service}' is not clean. Commit or stash your changes before editing variables."
        )

    # Get the variable file to edit
    entity = create_entity_name(service, component)
    variables_file = vars / service / (entity.name + YML_EXTENSION)

    logger.debug(f"Editing {entity.name} for service {service}")

    # Loop until variables file format has no errors or user aborts editing file
    while True:
        click.edit(
            filename=str(variables_file),
        )

        # Pause until user press any key
        value: str = click.prompt(
            "Press any key when done editing ('q' to cancel)",
            type=str,
            prompt_suffix="",
            default="continue",
            show_default=False,
        )
        if value.lower() == "q":
            repo.restore_file(str(variables_file))
            raise click.ClickException("Aborted changes")

        # Check schema
        try:
            service_variables.validate()
        except InvalidSchemaError:
            click.echo(f"Variables does not match '{service}' schema")
            continue

        # Commit
        try:
            with repo.validate(commit_message):
                repo.add_for_validation([str(variables_file)])
        except EmptyCommit:
            raise click.ClickException("Nothing changed")

        click.echo(f"{variables_file.name} successfully updated")

        # Generate stale component list and save it to the database
        with Dao(db_engine) as dao:
            try:
                stale_status_logs = dao.get_cluster_status().generate_stale_sch_logs(
                    cluster_variables=cluster_variables, collections=collections
                )
            except ServiceVariablesNotInitializedErrorList as e:
                click.echo(str(e))
                click.echo("Their status will not be updated.")
            dao.session.add_all(stale_status_logs)
            dao.session.commit()

        break
