# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.queries import (
    get_sch_status,
)
from tdp.cli.session import get_session
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    print_table,
    validate,
    vars,
)
from tdp.core.cluster_status import ClusterStatus, SCHStatus
from tdp.core.models.sch_status_log import SCHStatusLog, SCHStatusLogSourceEnum
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.collections import Collections


@click.command(short_help="List stale components.")
@click.argument("service", nargs=1, required=False)
@click.argument("component", nargs=1, required=False)
@collections
@database_dsn
@click.option(
    "--generate-stales", is_flag=True, help="Update the list of stale components."
)
@click.option(
    "--host",
    "hosts",
    envvar="TDP_HOSTS",
    type=str,
    multiple=True,
    help="Hosts where components are defined. Can be used multiple times.",
)
@click.option("--override", "-O", is_flag=True, help="Override the stale status.")
@click.option("--stale", is_flag=True, help="Only print stale components.")
@click.option(
    "--to-config",
    type=bool,
    help="To be used with --override. Override to_config value.",
)
@click.option(
    "--to-restart",
    type=bool,
    help="To be used with --override. Override to_restart value.",
)
@validate
@vars
def status(
    service: Optional[str],
    component: Optional[str],
    collections: Collections,
    database_dsn: str,
    generate_stales: bool,
    hosts: Optional[Iterable[str]],
    override: bool,
    stale: bool,
    to_config: Optional[bool],
    to_restart: Optional[bool],
    validate: bool,
    vars: Path,
):
    # Check if the options are valid
    if override and generate_stales:
        raise click.UsageError(
            "The --override and --generate-stales options are mutually exclusive."
        )
    if (to_config is not None or to_restart is not None) and not override:
        raise click.UsageError(
            "The --to-config and --to-restart options require --override option."
        )

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    # Check if the service exist
    if service and service not in cluster_variables.keys():
        raise click.UsageError(f"Service '{service}' does not exists.")

    # Check if the component exist
    if (
        service
        and component
        and component in collections.get_components_from_service(service)
    ):
        raise click.UsageError(
            f"Component '{component}' does not exists in service '{service}'."
        )

    with get_session(database_dsn) as session:
        if override:
            # Check if the options are valid
            if not service or not hosts:
                raise click.UsageError(
                    "The --override option requires at least a service."
                )
            if to_config is None and to_restart is None:
                raise click.UsageError(
                    "Nothing to override. Use --to-config and/or --to-restart options."
                )

            # Create a new SCHStatusLog for each host
            for host in hosts:
                session.add(
                    SCHStatusLog(
                        service=service,
                        component=component,
                        host=host,
                        to_config=to_config,
                        to_restart=to_restart,
                        source=SCHStatusLogSourceEnum.MANUAL,
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

        elif generate_stales:
            click.echo("Updating the list of stale components.")
            stale_status_logs = ClusterStatus.from_sch_status_rows(
                get_sch_status(session)
            ).generate_stale_sch_logs(
                cluster_variables=cluster_variables, collections=collections
            )
            session.add_all(stale_status_logs)
            session.commit()

        _print_sch_status_logs(
            ClusterStatus.from_sch_status_rows(
                get_sch_status(session)
            ).find_sch_statuses(
                service=service, component=component, hosts=hosts, stale=stale
            )
        )


def _print_sch_status_logs(sch_status: Iterable[SCHStatus]) -> None:
    print_table(
        [status.to_dict(filter_out=["id", "timestamp"]) for status in sch_status],
    )
