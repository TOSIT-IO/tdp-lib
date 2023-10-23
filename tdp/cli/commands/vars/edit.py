# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path

import click

from tdp.cli.queries import get_sch_status
from tdp.cli.session import get_session
from tdp.cli.utils import (
    collections,
    database_dsn,
    validate,
    vars,
)
from tdp.core.cluster_status import ClusterStatus
from tdp.core.collection import YML_EXTENSION
from tdp.core.collections import Collections
from tdp.core.repository.repository import EmptyCommit
from tdp.core.service_component_name import ServiceComponentName
from tdp.core.variables import ClusterVariables
from tdp.core.variables.service_variables import InvalidSchema

logger = logging.getLogger(__name__)


@click.command()
@click.argument("service_name", nargs=1, required=True)
@click.argument("service_component_parameter", nargs=1, required=False)
@click.option(
    "--commit_message",
    "-c",
    type=str,
    default="updated from tdp edit-vars command",
    help="Validation message for the service repository",
)
@collections
@database_dsn
@validate
@vars
def edit(
    service_name: str,
    service_component_parameter: str,
    commit_message: str,
    collections: Collections,
    database_dsn: str,
    validate: bool,
    vars: Path,
):
    """Edit a variables file.

    SERVICE_NAME: Name of the service to edit variables for.
    SERVICE_COMPONENT_PARAMETER: Name or path of the component to edit variables for. If not specified, edit the service variables file.

    \b
    Examples:
        tdp edit-vars hdfs
        tdp edit-vars hdfs datanode --commit_message "updated datanode variables"
        tdp edit-vars hdfs hdfs_datanode.yml
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
        # tdp edit-vars service
        variables_file = base_path / (service_name + YML_EXTENSION)
    elif service_component_parameter.endswith(YML_EXTENSION):
        # tdp edit-vars service service.yml OR tdp edit-vars service service_component.yml
        variables_file = base_path / service_component_parameter
    else:
        # tdp edit-vars service component
        variables_file = base_path / (
            service_name + "_" + service_component_parameter + YML_EXTENSION
        )

    # Check if component exists
    service_component_name = ServiceComponentName.from_full_name(variables_file.stem)
    if not service_component_name.is_service:
        components_of_service = collections.get_components_from_service(service_name)
        if service_component_name not in components_of_service:
            raise click.ClickException(
                f"Error unknown component '{service_component_name.component_name}' for service '{service_component_name.service_name}'"
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
        except InvalidSchema:
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
        with get_session(database_dsn) as session:
            stale_status_logs = ClusterStatus.from_sch_status_rows(
                get_sch_status(session)
            ).generate_stale_sch_logs(
                cluster_variables=cluster_variables, collections=collections
            )
            session.add_all(stale_status_logs)
            session.commit()

        break
