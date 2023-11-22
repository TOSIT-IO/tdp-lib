# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.commands.status.utils import (
    _common_status_options,
    _print_sch_status_logs,
)
from tdp.cli.queries import get_sch_status
from tdp.cli.session import get_session
from tdp.cli.utils import check_services_cleanliness, hosts
from tdp.core.cluster_status import ClusterStatus
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from pathlib import Path

    from tdp.core.collections import Collections


@click.command()
@_common_status_options
@hosts(help="Host to filter. Can be used multiple times.")
@click.option("--stale", is_flag=True, help="Only print stale components.")
def show(
    collections: Collections,
    database_dsn: str,
    hosts: tuple[str],
    stale: bool,
    validate: bool,
    vars: Path,
    service: Optional[str] = None,
    component: Optional[str] = None,
) -> None:
    """Print the status of the cluster.

    Provide a SERVICE and a COMPONENT to filter the results.
    """
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars_path=vars, validate=validate
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
