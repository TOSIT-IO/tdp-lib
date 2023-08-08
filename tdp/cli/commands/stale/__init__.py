# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import List
from pathlib import Path

import click
from tabulate import tabulate

from tdp.cli.queries import (
    get_latest_success_component_version_log,
    get_stale_components,
)
from tdp.cli.session import get_session_class
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    validate,
    vars,
)
from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.models import StaleComponent
from tdp.core.variables import ClusterVariables


@click.command(short_help="List stale components.")
@collections
@database_dsn
@click.option("--generate", is_flag=True, help="Generate the list of stale components.")
@validate
@vars
def stale(
    collections: Collections, database_dsn: str, generate: bool, validate, vars: Path
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist.")
    dag = Dag(collections)

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    session_class = get_session_class(database_dsn)
    with session_class() as session:
        if generate:
            click.echo("Generating the list of stale components.")
            deployed_component_version_logs = get_latest_success_component_version_log(
                session
            )
            stale_components = StaleComponent.generate(
                dag, cluster_variables, deployed_component_version_logs
            )
            session.query(StaleComponent).delete()
            session.add_all(stale_components)
            session.commit()
        # Print stale components
        stale_components = get_stale_components(session)
        _print_stale_components(stale_components)


def _print_stale_components(stale_components: List[StaleComponent]):
    """Print the list of stale components.

    Args:
        stale_components: The list of stale components to print.
    """
    headers = StaleComponent.__table__.columns.keys()
    click.echo(
        tabulate(
            [
                _format_stale_component(stale_component, headers)
                for stale_component in stale_components
            ],
            headers="keys",
        )
    )


def _format_stale_component(
    stale_component: StaleComponent, headers: List[str]
) -> dict:
    """Format a StaleComponent object into a dict for tabulate.

    Args:
        stale_component: The StaleComponent object to format.
        headers: The headers to use for the tabulate table.

    Returns:
        A dict with the formatted StaleComponent object.
    """

    return {key: str(getattr(stale_component, key)) for key in headers}
