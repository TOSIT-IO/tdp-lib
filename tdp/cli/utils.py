# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import re
from collections.abc import Iterable
from logging import getLogger
from typing import TYPE_CHECKING, Literal, Optional

import click
from tabulate import tabulate

from tdp.core.models.enums import DeploymentStateEnum

if TYPE_CHECKING:
    from tdp.core.collections.collections import Collections
    from tdp.core.entities.hosted_entity_status import HostedEntityStatus
    from tdp.core.models import DeploymentModel
    from tdp.core.models.operation_model import OperationModel
    from tdp.core.variables.cluster_variables import ClusterVariables

logger = getLogger(__name__)


def validate_service_component(
    service: str, component: Optional[str] = None, *, collections: Collections
):
    """Wraps the validation of service and component arguments to display a
    user-friendly error message."""
    try:
        collections.validate_service_component(service, component)
    except ValueError as e:
        raise click.UsageError(
            f"Invalid SERVICE or COMPONENT argument: {f'{service}_{component}' if component else service}"
        ) from e


def check_services_cleanliness(cluster_variables: ClusterVariables) -> None:
    """Check that all services are in a clean state.

    Args:
        cluster_variables: Instance of ClusterVariables.

    Raises:
        click.ClickException: If some services are in a dirty state.
    """
    unclean_services = [
        service_variables.name
        for service_variables in cluster_variables.values()
        if not service_variables.clean
    ]
    if unclean_services:
        for name in unclean_services:
            click.echo(
                f'"{name}" repository is not in a clean state.'
                " Check that all modifications are committed."
            )
        raise click.ClickException(
            "Some services are in a dirty state, commit your modifications."
        )


def print_deployment(
    deployment: DeploymentModel, /, *, filter_out: Optional[list[str]] = None
) -> None:
    # Print general deployment infos
    click.secho("Deployment details", bold=True)
    click.echo(
        tabulate(
            deployment.to_dict(filter_out=filter_out).items(),
            tablefmt="plain",
        )
    )

    # Print deployment operations
    click.secho("\nOperations", bold=True)
    print_operations(
        deployment.operations, filter_out=[*(filter_out or []), "deployment_id"]
    )


def print_operations(
    operations: Iterable[OperationModel], /, *, filter_out: Optional[list[str]] = None
) -> None:
    """Print a list of operations in a human readable format.

    Args:
        operations: List of operations to print.
    """
    click.echo(
        tabulate(
            [o.to_dict(filter_out=filter_out) for o in operations],
            headers="keys",
        )
    )


def print_hosted_entity_status_log(sch_status: Iterable[HostedEntityStatus]) -> None:
    click.echo(
        tabulate(
            [status.export_tabulate() for status in sch_status],
            headers="keys",
        )
    )


def _parse_line(line: str) -> tuple[str, Optional[str], Optional[list[str]]]:
    """Parses a line which contains an operation, and eventually a host and extra vars.

    Args:
        line: Line to be parsed.

    Returns:
        Operation, host and extra vars.
    """
    parsed_line = re.match(
        r"^(.*?)( on .*?){0,1}( ?with .*?){0,1}( ?on .*?){0,1}$", line
    )

    if parsed_line is None:
        raise ValueError(
            "Error on line '"
            + line
            + "': it must be 'OPERATION [on HOST] [with EXTRA_VARS[,EXTRA_VARS]].'"
        )

    if parsed_line.group(1).split(" ")[0] == "":
        raise ValueError("Error on line '" + line + "': it must contain an operation.")

    if len(parsed_line.group(1).strip().split(" ")) > 1:
        raise ValueError("Error on line '" + line + "': only 1 operation is allowed.")

    if parsed_line.group(2) is not None and parsed_line.group(4) is not None:
        raise ValueError(
            "Error on line '" + line + "': only 1 host is allowed in a line."
        )

    operation = parsed_line.group(1).strip()

    # Get the host and test if it is declared
    if parsed_line.group(2) is not None:
        host = parsed_line.group(2).split(" on ")[1]
        if host == "":
            raise ValueError(
                "Error on line '" + line + "': host is required after 'on' keyword."
            )
    elif parsed_line.group(4) is not None:
        host = parsed_line.group(4).split(" on ")[1]
        if host == "":
            raise ValueError(
                "Error on line '" + line + "': host is required after 'on' keyword."
            )
    else:
        host = None

    # Get the extra vars and test if they are declared
    if parsed_line.group(3) is not None:
        extra_vars = parsed_line.group(3).split(" with ")[1]
        if extra_vars == "":
            raise ValueError("Extra vars are required after 'with' keyword.")
        extra_vars = extra_vars.split(",")
        extra_vars = [item.strip() for item in extra_vars]
    else:
        extra_vars = None

    return (operation, host, extra_vars)


def parse_file(file_name) -> list[tuple[str, Optional[str], Optional[list[str]]]]:
    """Parses a file which contains operations, hosts and extra vars."""
    file_content = file_name.read()
    return [
        _parse_line(line)
        for line in file_content.split("\n")
        if line and not line.startswith("#")
    ]


def validate_clean_last_deployment_state(
    last_deployment_state: Optional[DeploymentStateEnum], force: Optional[bool] = None
) -> Literal[
    DeploymentStateEnum.SUCCESS,
    DeploymentStateEnum.FAILURE,
    DeploymentStateEnum.PLANNED,
]:
    """Validates that a new deployment plan can be created.

    Args:
        status: Status of the last deployment.
        force: Whether to force the creation of a new deployment plan.

    Raises:
        click.ClickException: If a new deployment plan cannot be created.
    """
    # Deployment must have a state
    # TODO: remove optional on DeploymentModel.state
    if last_deployment_state is None:
        raise click.ClickException("Unknown deployment state. This shouldn't happen.")

    # OK if last deployment is FAILURE OR SUCCESS
    if last_deployment_state in (
        DeploymentStateEnum.SUCCESS,
        DeploymentStateEnum.FAILURE,
    ):
        return last_deployment_state

    # OK if forced
    if force:
        logger.debug("Force option enabled, overriding existing deployment plan.")
        return DeploymentStateEnum.PLANNED

    # Ask to confirm overriding if last deployment is PLANNED
    if last_deployment_state == DeploymentStateEnum.PLANNED:
        click.confirm(
            "A deployment plan already exists, do you want to override it?",
            abort=True,
        )
        return DeploymentStateEnum.PLANNED

    # Display an error is last deployment is RUNNING
    if last_deployment_state == DeploymentStateEnum.RUNNING:
        raise click.ClickException(
            "Last deployment is in a RUNNING state. Wait for it to finish "
            "before planning a new deployment.\n\n"
            "Use '--force' to create a plan anyway (not recommended)."
        )

    raise click.ClickException("Unknown deployment state.")
