# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
import re
from pathlib import Path

import click

from tdp.cli.commands.queries import get_latest_success_service_version_query
from tdp.cli.session import get_session_class
from tdp.cli.utils import check_services_cleanliness, collection_paths_to_collections
from tdp.core.dag import Dag
from tdp.core.runner import (
    AnsibleExecutor,
    DeploymentPlan,
    DeploymentRunner,
    EmptyDeploymentPlanError,
)
from tdp.core.variables import ClusterVariables


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
@click.option("--dry", is_flag=True, help="Execute dag without running any action")
def restart_required(
    database_dsn,
    collections,
    run_directory,
    vars,
    dry,
):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collections)
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

        cluster_variables = ClusterVariables.get_cluster_variables(vars)
        check_services_cleanliness(cluster_variables)

        components_modified = set()
        for deployment_id, service, version in latest_success_service_version:
            if service not in cluster_variables:
                raise RuntimeError(
                    f"Service '{service}' is deployed but the repository is missing."
                )
            for component_modified in cluster_variables[service].components_modified(
                dag, version
            ):
                components_modified.add(component_modified.name)

        if len(components_modified) == 0:
            click.echo("Nothing needs to be restarted")
            return

        deployment_runner = DeploymentRunner(
            collections, ansible_executor, cluster_variables
        )
        try:
            deployment_plan = DeploymentPlan.from_dag(
                dag,
                sources=list(components_modified),
                restart=True,
                filter_expression=re.compile(r".+_(config|start)"),
            )
        except EmptyDeploymentPlanError:
            raise click.ClickException(
                f"Component(s) [{', '.join(components_modified)}] don't have any operation associated to restart (excluding noop). Nothing to restart."
            )
        deployment_iterator = deployment_runner.run(deployment_plan)
        if dry:
            for operation in deployment_iterator:
                pass
        else:
            session.add(deployment_iterator.log)
            # insert pending deployment log
            session.commit()
            for operation in deployment_iterator:
                session.add(operation)
                session.commit()
            # notify sqlalchemy deployment log has been updated
            session.merge(deployment_iterator.log)
            session.commit()
