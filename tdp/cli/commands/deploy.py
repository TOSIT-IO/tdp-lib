# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click

from tdp.cli.queries import (
    get_planned_deployment,
)
from tdp.cli.utils import (
    check_services_cleanliness,
    collections,
    database_dsn,
    validate,
    vars,
)
from tdp.core.cluster_status import ClusterStatus
from tdp.core.deployment import DeploymentRunner, Executor
from tdp.core.models.enums import DeploymentStateEnum
from tdp.core.variables import ClusterVariables
from tdp.dao import Dao

if TYPE_CHECKING:
    from tdp.core.collections import Collections


@click.command()
@click.option(
    "--force-stale-update",
    "--fsu",
    "force_stale_update",
    is_flag=True,
    help="Force stale status update.",
)
@click.option("--dry", is_flag=True, help="Execute dag without running any action.")
@collections
@database_dsn
@click.option(
    "--mock-deploy",
    envvar="TDP_MOCK_DEPLOY",
    is_flag=True,
    help="Mock the deploy, do not actually run the ansible playbook.",
)
@click.option(
    "--run-directory",
    envvar="TDP_RUN_DIRECTORY",
    type=click.Path(resolve_path=True, path_type=Path, exists=True),
    help="Working directory where the executor is launched (`ansible-playbook` for Ansible).",
    required=True,
)
@validate
@vars
def deploy(
    dry: bool,
    collections: Collections,
    database_dsn: str,
    force_stale_update: bool,
    mock_deploy: bool,
    run_directory: Path,
    validate: bool,
    vars: Path,
):
    """Execute a planned deployment."""
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections, vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with Dao(database_dsn, commit_on_exit=True) as dao:
        planned_deployment = get_planned_deployment(dao.session)
        if planned_deployment is None:
            raise click.ClickException(
                "No planned deployment found, please run `tdp plan` first."
            )

        deployment_iterator = DeploymentRunner(
            collections=collections,
            executor=Executor(
                run_directory=run_directory.absolute() if run_directory else None,
                dry=dry or mock_deploy,
            ),
            cluster_variables=cluster_variables,
            cluster_status=ClusterStatus.from_sch_status_rows(dao.get_sch_status()),
        ).run(planned_deployment, force_stale_update=force_stale_update)

        if dry:
            for _ in deployment_iterator:
                if _:
                    _()
            return

        # deployment and operations records are mutated by the iterator so we need to
        # commit them before iterating and at each iteration
        dao.session.commit()  # Update operation status to RUNNING
        for process_operation_fn in deployment_iterator:
            dao.session.commit()  # Update deployment and current operation status to RUNNING and next operations to PENDING
            if process_operation_fn and (cluster_status_logs := process_operation_fn()):
                dao.session.add_all(cluster_status_logs)
            dao.session.commit()  # Update operation status to SUCCESS, FAILURE or HELD

        if deployment_iterator.deployment.state != DeploymentStateEnum.SUCCESS:
            raise click.ClickException("Deployment failed.")
        else:
            click.echo("Deployment finished with success.")
