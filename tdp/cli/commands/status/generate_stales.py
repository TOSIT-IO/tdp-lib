# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.commands.status.utils import (
    _common_status_options,
    _print_sch_status_logs,
)
from tdp.cli.utils import check_services_cleanliness
from tdp.core.variables import ClusterVariables
from tdp.dao import Dao

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
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with Dao(database_dsn) as dao:
        stale_status_logs = dao.get_sch_status().generate_stale_sch_logs(
            cluster_variables=cluster_variables, collections=collections
        )

        dao.session.add_all(stale_status_logs)
        dao.session.commit()

        _print_sch_status_logs(
            dao.get_sch_status(service, component, include_not_stale=False).values()
        )
