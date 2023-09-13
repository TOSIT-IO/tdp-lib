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
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.collections import Collections


# TODO: Display a warning if service or component doesn't exist
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
    envvar="TDP_HOSTS",
    type=str,
    multiple=True,
    help="Hosts where components are defined. Can be used multiple times.",
)
@click.option("--stale", is_flag=True, help="Only print stale components.")
@validate
@vars
def status(
    service: Optional[str],
    component: Optional[str],
    collections: Collections,
    database_dsn: str,
    generate_stales: bool,
    host: Optional[Iterable[str]],
    stale: bool,
    validate: bool,
    vars: Path,
):
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with get_session(database_dsn) as session:
        if generate_stales:
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
                service=service, component=component, hosts=host, stale=stale
            )
        )


def _print_sch_status_logs(sch_status: Iterable[SCHStatus]) -> None:
    print_table(
        [status.to_dict(filter_out=["id", "timestamp"]) for status in sch_status],
    )
