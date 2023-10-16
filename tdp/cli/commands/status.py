# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click
from click.decorators import FC

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


def _check_service(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[str]:
    """Click callback that check if the service exists."""
    collections: Collections = ctx.params["collections"]
    vars: Path = ctx.params["vars"]
    # TODO: would be nice if services can be retrieved from the collections
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=False
    )
    if value and value not in cluster_variables.keys():
        raise click.UsageError(f"Service '{value}' does not exists.")
    return value


def _check_component(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[str]:
    """Click callback that check if the component exists."""
    collections: Collections = ctx.params["collections"]
    service: str = ctx.params["service"]
    if value and value not in [
        sc_name.component_name
        for sc_name in collections.get_components_from_service(service)
    ]:
        raise click.UsageError(
            f"Component '{value}' does not exists in service '{service}'."
        )
    return value


def _hosts(func: FC) -> FC:
    return click.option(
        "--host",
        "hosts",
        envvar="TDP_HOSTS",
        type=str,
        multiple=True,
        help="Host to filter. Can be used multiple times.",
    )(func)


def _common_status_options(func: FC) -> FC:
    """Add common status options to the command."""
    for option in reversed(
        [
            click.argument("service", nargs=1, required=False, callback=_check_service),
            click.argument(
                "component", nargs=1, required=False, callback=_check_component
            ),
            collections,
            database_dsn,
            validate,
            vars,
        ]
    ):
        func = option(func)  # type: ignore
    return func


@click.group()
def status() -> None:
    """Manage the status of the cluster."""
    pass


@status.command()
@_common_status_options
@_hosts
@click.option("--stale", is_flag=True, help="Only print stale components.")
def show(
    service: Optional[str],
    component: Optional[str],
    collections: Collections,
    database_dsn: str,
    hosts: Optional[Iterable[str]],
    stale: bool,
    validate: bool,
    vars: Path,
) -> None:
    """Print the status of the cluster.

    Provide a SERVICE and a COMPONENT to filter the results.
    """
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with get_session(database_dsn) as session:
        _print_sch_status_logs(
            ClusterStatus.from_sch_status_rows(
                get_sch_status(session)
            ).find_sch_statuses(
                service=service, component=component, hosts=hosts, stale=stale
            )
        )


@status.command()
@_common_status_options
def generate_stales(
    service: Optional[str],
    component: Optional[str],
    collections: Collections,
    database_dsn: str,
    hosts: Optional[Iterable[str]],
    validate: bool,
    vars: Path,
) -> None:
    """Generate stale components.

    Stales components are components that have been modified and need to be
    reconfigured and/or restarted.
    """
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with get_session(database_dsn) as session:
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
                service=service, component=component, hosts=hosts, stale=True
            )
        )


@status.command()
@_common_status_options
@_hosts
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
    service: Optional[str],
    component: Optional[str],
    collections: Collections,
    database_dsn: str,
    hosts: Optional[Iterable[str]],
    to_config: Optional[bool],
    to_restart: Optional[bool],
    validate: bool,
    vars: Path,
) -> None:
    """Edit the status of the cluster.

    Provide a SERVICE and a COMPONENT (optional) to edit.
    """
    if to_config is not None and to_restart is not None:
        raise click.UsageError(
            "You must provide either --to-config or --to-restart option."
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
            raise click.UsageError("At least one --host is required.")

        # Create a new SCHStatusLog for each host
        for host in hosts:
            session.add(
                SCHStatusLog(
                    service=service,
                    component=component,
                    host=host,
                    source=SCHStatusLogSourceEnum.MANUAL,
                    to_config=to_config,
                    to_restart=to_restart,
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


def _print_sch_status_logs(sch_status: Iterable[SCHStatus]) -> None:
    print_table(
        [status.to_dict(filter_out=["id", "timestamp"]) for status in sch_status],
    )
