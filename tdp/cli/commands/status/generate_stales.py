# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.params import (
    collections_option,
    component_argument_option,
    database_dsn_option,
    service_argument_option,
    validate_option,
    vars_option,
)
from tdp.cli.utils import validate_service_component

if TYPE_CHECKING:
    from pathlib import Path

    from sqlalchemy import Engine

    from tdp.core.collections import Collections


@click.command()
@service_argument_option
@component_argument_option
@collections_option
@database_dsn_option
@validate_option
@vars_option
def generate_stales(
    collections: Collections,
    db_engine: Engine,
    validate: bool,
    vars: Path,
    service: Optional[str] = None,
    component: Optional[str] = None,
) -> None:
    """Generate stale components.

    Stales components are components that have been modified and need to be
    reconfigured and/or restarted.
    """
    from tdp.cli.utils import check_services_cleanliness, print_hosted_entity_status_log
    from tdp.core.exceptions import ServiceVariablesNotInitializedErrorList
    from tdp.core.variables import ClusterVariables
    from tdp.dao import Dao

    if not service and component:
        raise click.UsageError(
            "Component argument cannot be used without a service argument."
        )
    elif service:
        validate_service_component(service, component, collections=collections)

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with Dao(db_engine) as dao:
        try:
            stale_status_logs = dao.get_cluster_status().generate_stale_sch_logs(
                cluster_variables=cluster_variables, collections=collections
            )
        except ServiceVariablesNotInitializedErrorList as e:
            click.echo(str(e))
            click.echo("Their status will not be updated.")

        dao.session.add_all(stale_status_logs)
        dao.session.commit()

        print_hosted_entity_status_log(
            dao.get_hosted_entity_statuses(service, component, filter_stale=True)
        )
