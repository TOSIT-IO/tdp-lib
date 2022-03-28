from pathlib import Path

import click

from tdp.cli.context import pass_dag
from tdp.cli.session import init_db
from tdp.core.repository.repository import NoVersionYet
from tdp.core.service_manager import ServiceManager


@click.command(short_help="Init database / services in tdp vars")
@click.option(
    "--sqlite-path",
    envvar="TDP_SQLITE_PATH",
    required=True,
    type=Path,
    help="Path to SQLITE database file",
)
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    type=Path,
    help="Path to tdp-collection",
)
@click.option(
    "--vars", envvar="TDP_VARS", required=True, type=Path, help="Path to the tdp vars"
)
@pass_dag
def init(dag, sqlite_path, collection_path, vars):
    init_db(sqlite_path)
    default_vars = collection_path / "tdp_vars_defaults"
    service_managers = ServiceManager.initialize_service_managers(
        dag, vars, default_vars
    )
    for name, service_manager in service_managers.items():
        try:
            click.echo(f"{name}: {service_manager.version}")
        except NoVersionYet:
            click.echo(f"Initializing {name}")
            service_manager.initiliaze_variables(service_manager)
