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
from tdp.cli.utils import check_services_cleanliness
from tdp.core.cluster_status import ClusterStatus
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from pathlib import Path

    from tdp.core.collections import Collections


@click.command()
@_common_status_options
def generate_stales(
    collections: Collections,
    database_dsn: str,
    validate: bool,
    vars: Path,
    service: Optional[str] = None,
    component: Optional[str] = None,
) -> None:
    """Generate stale components.

    Stales components are components that have been modified and need to be
    reconfigured and/or restarted.
    """
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars_path=vars, validate=validate
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
            ).find_sch_statuses(service=service, component=component, stale=True)
        )
