# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from typing import List
from tabulate import tabulate

from tdp.cli.queries import get_latest_success_component_version_log
from tdp.core.variables import ClusterVariables
from tdp.cli.session import get_session_class
from tdp.core.dag import Dag
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    validate,
    vars,
)
from tdp.core.models import StaleComponent


@click.command(short_help="List stale components.")
@database_dsn
@vars
@collections
@click.option("--generate", is_flag=True, help="Generate the list of stale components.")
def stale(database_dsn: str, generate: bool, vars, collections):
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
            stale_components_dict = StaleComponent.generate(
                dag, cluster_variables, deployed_component_version_logs
            )
            session.query(StaleComponent).delete()
            session.add_all(list(stale_components_dict.values()))
            session.commit()
        # Print stale components
        stale_components = session.query(StaleComponent).all()
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
