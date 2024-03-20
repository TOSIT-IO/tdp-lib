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
from tdp.core.variables import ClusterVariables
from tdp.dao import Dao

if TYPE_CHECKING:
    from pathlib import Path

    from tdp.core.collections import Collections


def _filter_stale(stale: Optional[bool], no_stale: Optional[bool]) -> Optional[bool]:
    """Return the filter_stale argument for get_sch_status."""
    if stale is False and no_stale is False:
        raise click.UsageError("Either --stale or --no-stale must be True.")
    elif stale and not no_stale:
        return True
    elif no_stale and not stale:
        return False
    else:
        return None


@click.command()
@_common_status_options
@hosts(help="Host to filter. Can be used multiple times.")
@click.option("--stale", is_flag=True, default=None, help="Filter stale components.")
@click.option(
    "--no-stale", is_flag=True, default=None, help="Filter non stale components."
)
def show(
    collections: Collections,
    database_dsn: str,
    hosts: tuple[str],
    stale: bool,
    no_stale: bool,
    validate: bool,
    vars: Path,
    service: Optional[str] = None,
    component: Optional[str] = None,
) -> None:
    """Print the status of the cluster.

    Provide a SERVICE and a COMPONENT to filter the results.

    --stale/--no-stale is used to select only stale or non-stale components. By default,
    both are printed (same as using both flags).
    """
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with Dao(database_dsn) as dao:
        _print_sch_status_logs(
            dao.get_sch_status(
                service, component, hosts, filter_stale=_filter_stale(stale, no_stale)
            ).values()
        )
