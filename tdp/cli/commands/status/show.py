# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Optional

import click
from sqlalchemy import Engine

from tdp.cli.params import (
    collections_option,
    database_dsn_option,
    hosts_option,
    validate_option,
    vars_option,
)
from tdp.cli.params.status import (
    component_argument_option,
    service_argument_option,
)
from tdp.cli.utils import (
    check_services_cleanliness,
    print_hosted_entity_status_log,
    print_table,
)
from tdp.core.models.sch_status_log_model import SCHStatusLogModel
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


def _filter_active(active: Optional[bool], inactive: Optional[bool]) -> Optional[bool]:
    """Return the filter_active argument for get_sch_status."""
    if active is False and inactive is False:
        raise click.UsageError("Either --active or --inactive must be True.")
    elif active and inactive:
        return None
    elif inactive and not active:
        return False
    else:
        return True


@click.command()
@service_argument_option
@component_argument_option
@collections_option
@database_dsn_option
@validate_option
@vars_option
@hosts_option(help="Host to filter. Can be used multiple times.")
@click.option("--stale", is_flag=True, default=None, help="Filter stale components.")
@click.option(
    "--no-stale", is_flag=True, default=None, help="Filter non stale components."
)
@click.option("--active", is_flag=True, default=None, help="Filter active components.")
@click.option(
    "--inactive", is_flag=True, default=None, help="Filter inactive components."
)
@click.option(
    "--history",
    is_flag=True,
    help="The history of editions for all or specific service and component.",
)
@click.option(
    "--limit",
    envvar="TDP_HISTORY_LIMIT",
    type=int,
    default=50,
    help="Limit number of lines returned with option --history.",
)
def show(
    collections: Collections,
    db_engine: Engine,
    hosts: tuple[str],
    stale: bool,
    no_stale: bool,
    active: Optional[bool],
    inactive: Optional[bool],
    history: bool,
    limit: int,
    validate: bool,
    vars: Path,
    service: Optional[str] = None,
    component: Optional[str] = None,
) -> None:
    """Print the status of the cluster.

    Provide a SERVICE and a COMPONENT to filter the results.

    --stale/--no-stale is used to select only stale or non-stale components. By default,
    both are printed (same as using both flags).

    --active/--inactive is used to select only active or inactive components. By default
    only active components are printed.
    """
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with Dao(db_engine) as dao:
        if history:
            _print_sch_status_logs(
                dao.get_hosted_entity_statuses_history(
                    limit,
                    service,
                    component,
                    hosts,
                    filter_stale=_filter_stale(stale, no_stale),
                    filter_active=_filter_active(active, inactive),
                )
            )
            return

        print_hosted_entity_status_log(
            dao.get_hosted_entity_statuses(
                service,
                component,
                hosts,
                filter_stale=_filter_stale(stale, no_stale),
                filter_active=_filter_active(active, inactive),
            )
        )


def _print_sch_status_logs(sch_status: Iterable[SCHStatusLogModel]) -> None:
    print_table(
        [status.to_dict(filter_out=["id", "timestamp"]) for status in sch_status],
    )
