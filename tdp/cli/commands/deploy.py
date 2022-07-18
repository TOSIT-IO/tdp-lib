# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click

from tdp.cli.session import get_session_class
from tdp.cli.utils import check_services_cleanliness, collection_paths
from tdp.core.dag import Dag
from tdp.core.runner.ansible_executor import AnsibleExecutor
from tdp.core.runner.operation_runner import OperationRunner
from tdp.core.service_manager import ServiceManager


@click.command(short_help="Deploy TDP")
@click.option(
    "--sources",
    type=str,
    metavar="s1,s2,...",
    help="Nodes where the run start (separate with comma)",
)
@click.option(
    "--targets",
    type=str,
    metavar="t1,t2,...",
    help="Nodes where the run stop (separate with comma)",
)
@click.option(
    "--database-dsn",
    envvar="TDP_DATABASE_DSN",
    required=True,
    type=str,
    help=(
        "Database Data Source Name, in sqlalchemy driver form "
        "example: sqlite:////data/tdp.db or sqlite+pysqlite:////data/tdp.db. "
        "You might need to install the relevant driver to your installation (such "
        "as psycopg2 for postgresql)"
    ),
)
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    callback=collection_paths,  # transforms list of path into list of Collection
    help=f"List of paths separated by your os' path separator ({os.pathsep})",
)
@click.option(
    "--run-directory",
    envvar="TDP_RUN_DIRECTORY",
    type=Path,
    help="Working directory where the executor is launched (`ansible-playbook` for Ansible)",
    required=True,
)
@click.option(
    "--vars", envvar="TDP_VARS", type=Path, help="Path to the tdp vars", required=True
)
@click.option("--filter", type=str, help="Glob on list name")
@click.option("--dry", is_flag=True, help="Execute dag without running any operation")
def deploy(
    sources,
    targets,
    database_dsn,
    collection_path,
    run_directory,
    vars,
    filter,
    dry,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collection_path)
    set_nodes = set()
    if sources:
        sources = sources.split(",")
        set_nodes.update(sources)
    if targets:
        targets = targets.split(",")
        set_nodes.update(targets)
    set_difference = set_nodes.difference(dag.operations)
    if set_difference:
        raise click.BadParameter(f"{set_difference} are not valid nodes")
    run_directory = run_directory.absolute() if run_directory else None

    ansible_executor = AnsibleExecutor(
        run_directory=run_directory,
        dry=dry,
    )
    session_class = get_session_class(database_dsn)
    with session_class() as session:
        service_managers = ServiceManager.get_service_managers(dag, vars)
        check_services_cleanliness(service_managers)

        operation_runner = OperationRunner(dag, ansible_executor, service_managers)
        if sources:
            click.echo(f"Deploying from {sources}")
        elif targets:
            click.echo(f"Deploying to {targets}")
        else:
            click.echo(f"Deploying TDP")
        deployment = operation_runner.run_nodes(
            sources=sources, targets=targets, node_filter=filter
        )
        session.add(deployment)
        session.commit()
