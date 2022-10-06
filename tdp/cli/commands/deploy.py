# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click

from tdp.cli.session import get_session_class
from tdp.cli.utils import check_services_cleanliness, collection_paths_to_collections
from tdp.cli.commands.browse import get_deployment
from tdp.core.dag import Dag
from tdp.core.runner.ansible_executor import AnsibleExecutor
from tdp.core.runner.operation_runner import OperationRunner
from tdp.core.variables import ClusterVariables


@click.command(short_help="Deploy TDP")
@click.option(
    "--sources",
    type=str,
    metavar="s1,s2,...",
    help="Nodes where the run start (separate with comma), cannot be used with --resume",
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
@click.option("--filter", type=str, help="Glob on list name")
@click.option("--dry", is_flag=True, help="Execute dag without running any operation")
@click.option(
    "--resume",
    type=str,
    metavar="dep_id",
    help="Resume a previous deployment, cannot be used with --sources",
)
def deploy(
    sources,
    targets,
    database_dsn,
    collections,
    run_directory,
    vars,
    filter,
    dry,
    resume,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collections)
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
        cluster_variables = ClusterVariables.get_cluster_variables(vars)
        check_services_cleanliness(cluster_variables)
        if resume:
            if sources:
                raise click.BadParameter(f"sources and resume can not be used together")
            resumed_deployment = get_deployment(session_class, resume)
            last_failed_operation = resumed_deployment.operations[-1].operation
            if resumed_deployment.operations[-1].state == "Success":
                raise click.BadParameter(
                    f"Nothing to resume, deployment n°{resume} was sucessful"
                )
            else:
                click.echo(f"Resuming deployment n°{resume}")
                sources = [last_failed_operation]
        operation_runner = OperationRunner(dag, ansible_executor, cluster_variables)
        if sources:
            click.echo(f"Deploying from {sources}")
        elif targets:
            click.echo(f"Deploying to {targets}")
        else:
            click.echo(f"Deploying TDP")
        try:
            operation_iterator = operation_runner.run_nodes(
                sources=sources, targets=targets, filter_expression=filter
            )
        except ValueError as e:
            raise click.ClickException(str(e)) from e
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
