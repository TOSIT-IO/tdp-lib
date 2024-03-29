# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.commands.status.utils import (
    _common_status_options,
    _print_sch_status_logs,
)
from tdp.cli.utils import check_services_cleanliness, hosts
from tdp.core.models.sch_status_log_model import (
    SCHStatusLogModel,
    SCHStatusLogSourceEnum,
)
from tdp.core.variables import ClusterVariables
from tdp.dao import Dao

if TYPE_CHECKING:
    from pathlib import Path

    from tdp.core.collections import Collections


@click.command()
@_common_status_options
@hosts(help="Host to filter. Can be used multiple times.")
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
@click.option(
    "--is-active",
    type=bool,
    help="Manually set the 'is_active' value.",
)
def edit(
    collections: Collections,
    database_dsn: str,
    vars: Path,
    validate: bool,
    hosts: tuple[str],
    service: Optional[str] = None,
    component: Optional[str] = None,
    message: Optional[str] = None,
    to_config: Optional[bool] = None,
    to_restart: Optional[bool] = None,
    is_active: Optional[bool] = None,
) -> None:
    """Edit the status of the cluster.

    Provide a SERVICE and a COMPONENT (optional) to edit.
    """
    if is_active is False and (to_config is not None or to_restart is not None):
        raise click.UsageError(
            "Setting `to-config` or `to-restart` won't have any effect if `is-active` is set to False."
        )
    if to_config is not None and to_restart is not None:
        raise click.UsageError(
            "You must provide either `--to-config` or `--to-restart` option."
        )

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with Dao(database_dsn) as dao:
        if not service:
            raise click.UsageError("SERVICE argument is required.")

        # TODO: would be nice if host is optional and we can edit all hosts at once
        if not hosts:
            raise click.UsageError("At least one `--host` is required.")

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
                    is_active=is_active,
                    message=message,
                )
            )

            # Print the override message
            override_msg = "Setting"
            if is_active is not None:
                override_msg += f" is_active to {is_active}"
            if to_config is not None:
                override_msg += f" to_config to {to_config}"
            if to_restart is not None:
                override_msg += " and" if to_config is not None else ""
                override_msg += f" to_restart to {to_restart}"
            override_msg += f" for {service}_{component} on {host}."
            click.echo(override_msg)

        dao.session.commit()

    with Dao(database_dsn) as dao:
        _print_sch_status_logs(
            dao.get_sch_status(
                service=service,
                component=component,
                hosts=hosts,
                filter_stale=True,
            ).values()
        )
