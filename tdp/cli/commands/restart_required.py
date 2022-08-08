# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
import re
from pathlib import Path

import click

from tdp.cli.commands.queries import get_latest_success_service_version_query
from tdp.cli.session import get_session_class
from tdp.cli.utils import check_services_cleanliness, collection_paths
from tdp.core.dag import Dag
from tdp.core.runner.ansible_executor import AnsibleExecutor
from tdp.core.runner.operation_runner import OperationRunner
from tdp.core.service_manager import ServiceManager


@click.command(short_help="Restart required TDP services")
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
    callback=collection_paths,  # transforms list of path into Collections
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
@click.option("--dry", is_flag=True, help="Execute dag without running any action")
def restart_required(
    database_dsn,
    collection_path,
    run_directory,
    vars,
    dry,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collection_path)
    run_directory = run_directory.absolute() if run_directory else None

    ansible_executor = AnsibleExecutor(
        run_directory=run_directory,
        dry=dry,
    )

    session_class = get_session_class(database_dsn)
    with session_class() as session:
        latest_success_service_version = session.execute(
            get_latest_success_service_version_query()
        ).all()

        service_managers = ServiceManager.get_service_managers(dag, vars)
        check_services_cleanliness(service_managers)

        components_modified = set()
        for deployment_id, service, version in latest_success_service_version:
            if service not in service_managers:
                raise RuntimeError(
                    f"Service '{service}' is deployed but the repository is missing."
                )
            for component_modified in service_managers[service].components_modified(
                version
            ):
                components_modified.add(component_modified.name)

        if len(components_modified) == 0:
            click.echo("Nothing needs to be restarted")
            return

        operation_runner = OperationRunner(dag, ansible_executor, service_managers)

        operation_iterator = operation_runner.run_nodes(
            sources=list(components_modified),
            restart=True,
            node_filter=re.compile(r".+_(config|start)"),
        )
        session.add(operation_iterator.deployment_log)
        # insert pending deployment log
        session.commit()
        for operation in operation_iterator:
            session.add(operation)
            session.commit()
        # notify sqlalchemy deployment log has been updated
        session.merge(operation_iterator.deployment_log)
        session.commit()
