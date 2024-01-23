# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.commands.status.utils import (
    _common_status_options,
    _print_sch_status_logs,
)
from tdp.cli.session import get_session
from tdp.cli.utils import check_services_cleanliness, hosts
from tdp.core.cluster_status import ClusterStatus
from tdp.core.models.sch_status_log_model import (
    SCHStatusLogModel,
    SCHStatusLogSourceEnum,
)
from tdp.core.variables import ClusterVariables
from tdp.queries import get_sch_status

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
) -> None:
    """Edit the status of the cluster.

    Provide a SERVICE and a COMPONENT (optional) to edit.
    """
    if to_config is not None and to_restart is not None:
        raise click.UsageError(
            "You must provide either `--to-config` or `--to-restart` option."
        )

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with get_session(database_dsn) as session:
        if not service:
            raise click.UsageError("SERVICE argument is required.")

        # TODO: would be nice if host is optional and we can edit all hosts at once
        if not hosts:
            raise click.UsageError("At least one `--host` is required.")

        # Create a new SCHStatusLog for each host
        for host in hosts:
            session.add(
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

        session.commit()

        _print_sch_status_logs(
            ClusterStatus.from_sch_status_rows(
                get_sch_status(session)
            ).find_sch_statuses(
                service=service, component=component, hosts=hosts, stale=False
            )
        )
