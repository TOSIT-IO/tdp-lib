# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Optional

import click
from sqlalchemy import Engine
from tabulate import tabulate

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
                )
            )
            return

        print_hosted_entity_status_log(
            dao.get_hosted_entity_statuses(
                service,
                component,
                hosts,
                filter_stale=_filter_stale(stale, no_stale),
            )
        )


def _print_sch_status_logs(sch_status: Iterable[SCHStatusLogModel]) -> None:
    click.echo(
        tabulate(
            [status.to_dict(filter_out=["id", "timestamp"]) for status in sch_status],
            headers="keys",
        )
    )
