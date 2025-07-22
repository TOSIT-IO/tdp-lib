# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import (
    collections_option,
    component_argument_option,
    database_dsn_option,
    hosts_option,
    service_argument_option,
    validate_option,
    vars_option,
)
from tdp.cli.utils import validate_service_component

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy import Engine

    from tdp.core.collections import Collections


@click.command()
@service_argument_option(required=True)
@component_argument_option
@collections_option
@database_dsn_option
@validate_option
@vars_option
@hosts_option(help="Host to filter. Can be used multiple times.")
@click.option(
    "--message",
    "-m",
    type=str,
    help="Description of the change.",
)
@click.option(
    "--to-config",
    type=bool,
    help="Manually set the 'to_config' value.",
)
@click.option(
    "--to-restart",
    type=bool,
    help="Manually set the 'to_restart' value.",
)
def edit(
    collections: Collections,
    db_engine: Engine,
    vars: Path,
    validate: bool,
    hosts: tuple[str],
    service: str,
    component: Optional[str] = None,
    message: Optional[str] = None,
    to_config: Optional[bool] = None,
    to_restart: Optional[bool] = None,
) -> None:
    """Edit the status of the cluster.

    Provide a SERVICE and a COMPONENT (optional) to edit.
    """

    from tdp.cli.utils import check_services_cleanliness, print_hosted_entity_status_log
    from tdp.core.models.sch_status_log_model import (
        SCHStatusLogModel,
        SCHStatusLogSourceEnum,
    )
    from tdp.core.variables import ClusterVariables
    from tdp.dao import Dao

    # Validate hosts option
    # TODO: would be nice if host is optional and we can edit all hosts at once
    if not hosts:
        raise click.UsageError("At least one `--host` is required.")
    # Validate service and component arguments
    validate_service_component(service, component, collections=collections)
    # Validate to_config and to_restart options
    if to_config is not None and to_restart is not None:
        raise click.UsageError(
            "You must provide either `--to-config` or `--to-restart` option."
        )

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with Dao(db_engine) as dao:
        # Create a new SCHStatusLog for each host
        for host in hosts:
            dao.session.add(
                SCHStatusLogModel(
                    service=service,
                    component=component,
                    host=host,
                    source=SCHStatusLogSourceEnum.MANUAL,
                    to_config=to_config,
                    to_restart=to_restart,
                    message=message,
                )
            )

            # Print the override message
            override_msg = "Setting"
            if to_config is not None:
                override_msg += f" to_config to {to_config}"
            if to_restart is not None:
                override_msg += " and" if to_config is not None else ""
                override_msg += f" to_restart to {to_restart}"
            override_msg += f" for {service}_{component} on {host}."
            click.echo(override_msg)

        dao.session.commit()

    with Dao(db_engine) as dao:
        print_hosted_entity_status_log(
            dao.get_hosted_entity_statuses(
                service=service,
                component=component,
                hosts=hosts,
                filter_stale=True,
            )
        )
