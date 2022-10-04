# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click

from tdp.cli.session import get_session_class
from tdp.cli.utils import check_services_cleanliness, collection_paths_to_collections
from tdp.core.dag import Dag
from tdp.core.runner.ansible_executor import AnsibleExecutor
from tdp.core.runner.operation_runner import OperationRunner
from tdp.core.variables import ClusterVariables


@click.command(short_help="Run single TDP operation")
@click.argument("operation_name")
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
    "collections",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    callback=collection_paths_to_collections,  # transforms into Collections object
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
    "--vars",
    envvar="TDP_VARS",
    required=True,
    type=click.Path(resolve_path=True, path_type=Path),
    help="Path to the tdp vars",
)
@click.option("--dry", is_flag=True, help="Execute without running any operation")
def run(
    operation_name,
    database_dsn,
    collections,
    run_directory,
    vars,
    dry,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collections)

    operation = dag.collections.operations.get(operation_name, None)
    if not operation:
        raise click.BadParameter(f"{operation_name} is not a valid operation")

    if operation.noop:
        raise click.BadParameter(
            f"{operation_name} is tagged as noop and thus"
            " cannot be executed in an unitary deployment"
        )

    run_directory = run_directory.absolute() if run_directory else None

    ansible_executor = AnsibleExecutor(
        run_directory=run_directory,
        dry=dry,
    )
    session_class = get_session_class(database_dsn)
    with session_class() as session:
        cluster_variables = ClusterVariables.get_cluster_variables(vars)
        check_services_cleanliness(cluster_variables)

        operation_runner = OperationRunner(dag, ansible_executor, cluster_variables)
        click.echo(f"Deploying {operation}")
        operation_iterator = operation_runner.run_operations([operation])
        if dry:
            for operation in operation_iterator:
                pass
        else:
            session.add(operation_iterator.deployment_log)
            # insert pending deployment log
            session.commit()
            for operation in operation_iterator:
                session.add(operation)
                session.commit()
            # notify sqlalchemy deployment log has been updated
            session.merge(operation_iterator.deployment_log)
            session.commit()
