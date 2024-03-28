# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from typing import Optional

import click
from sqlalchemy import Engine

from tdp.cli.params import (
    collections_option,
    database_dsn_option,
    validate_option,
    vars_option,
)
from tdp.core.collections import Collections
from tdp.core.constants import YML_EXTENSION
from tdp.core.entities.hostable_entity_name import (
    ServiceComponentName,
    parse_hostable_entity_name,
)
from tdp.core.repository.repository import EmptyCommit
from tdp.core.variables import ClusterVariables
from tdp.core.variables.schema.exceptions import InvalidSchemaError
from tdp.dao import Dao

logger = logging.getLogger(__name__)


@click.command()
@click.argument("service_name", nargs=1, required=True)
@click.argument("service_component_parameter", nargs=1, required=False)
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
    service_name: str,
    service_component_parameter: Optional[str] = None,
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
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections, vars, validate=validate
    )

    # Check if service exists
    if service_name not in cluster_variables:
        raise click.ClickException(f"Error unknown service '{service_name}'")

    service_variables = cluster_variables[service_name]
    repo = service_variables.repository

    # Check if service is clean
    if not service_variables.clean:
        raise click.ClickException(
            f"Error service '{service_name}' is not clean. Commit or stash your changes before editing variables."
        )

    # Get the variable file to edit
    base_path = vars / service_name
    if service_component_parameter is None:
        # tdp vars edit service
        variables_file = base_path / (service_name + YML_EXTENSION)
    elif service_component_parameter.endswith(YML_EXTENSION):
        # tdp vars edit service service.yml OR tdp vars edit service service_component.yml
        variables_file = base_path / service_component_parameter
    else:
        # tdp vars edit service component
        variables_file = base_path / (
            service_name + "_" + service_component_parameter + YML_EXTENSION
        )

    # Check if component exists
    entity_name = parse_hostable_entity_name(variables_file.stem)
    if isinstance(entity_name, ServiceComponentName):
        if entity_name not in collections.get_components_from_service(service_name):
            raise click.ClickException(
                f"Error unknown component '{entity_name.component}' for service '{entity_name.service}'"
            )

    logger.debug(f"Editing {variables_file.name} for service {service_name}")

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
            click.echo(f"Variables does not match '{service_name}' schema")
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
            stale_status_logs = dao.get_cluster_status().generate_stale_sch_logs(
                cluster_variables=cluster_variables, collections=collections
            )
            dao.session.add_all(stale_status_logs)
            dao.session.commit()

        break
