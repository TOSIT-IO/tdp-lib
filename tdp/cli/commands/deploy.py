# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click

from tdp.cli.params import (
    collections_option,
    database_dsn_option,
    validate_option,
    vars_option,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections


@click.group(invoke_without_command=True)
@click.pass_context
@click.option(
    "--force-stale-update",
    "--fsu",
    "force_stale_update",
    is_flag=True,
    help="Force stale status update.",
)
@click.option("--dry", is_flag=True, help="Execute dag without running any action.")
@collections_option
@database_dsn_option
@click.option(
    "--mock-deploy",
    envvar="TDP_MOCK_DEPLOY",
    is_flag=True,
    help="Mock the deploy, do not actually run the ansible playbook.",
)
@validate_option
@vars_option
def deploy(ctx, *args, **kwargs):
    """Execute a planned deployment."""
    if ctx.invoked_subcommand is None:
        _handle_deploy(*args, **kwargs)


def _handle_deploy(
    dry: bool,
    collections: Collections,
    db_engine: Engine,
    force_stale_update: bool,
    mock_deploy: bool,
    validate: bool,
    vars: Path,
):
    from tdp.cli.utils import check_services_cleanliness
    from tdp.core.deployment import DeploymentRunner, Executor
    from tdp.core.models.enums import DeploymentStateEnum
    from tdp.core.variables import ClusterVariables
    from tdp.dao import Dao

    cluster_variables = ClusterVariables.get_cluster_variables(
        collections, vars, validate=validate
    )
    check_services_cleanliness(cluster_variables)

    with Dao(db_engine, commit_on_exit=True) as dao:
        planned_deployment = dao.get_planned_deployment()
        if planned_deployment is None:
            raise click.ClickException(
                "No planned deployment found, please run `tdp plan` first."
            )

        deployment_iterator = DeploymentRunner(
            collections=collections,
            executor=Executor(dry=dry or mock_deploy),
            cluster_variables=cluster_variables,
            cluster_status=dao.get_cluster_status(),
        ).run(planned_deployment, force_stale_update=force_stale_update)

        if dry:
            for operation_rec, process_operation_fn in deployment_iterator:
                if process_operation_fn:
                    process_operation_fn()
                click.echo(
                    f"[DRY MODE]: Operation {operation_rec.operation} is {operation_rec.state}"
                )
            return

        # deployment and operations records are mutated by the iterator so we need to
        # commit them before iterating and at each iteration
        dao.session.commit()  # Update operation status to RUNNING
        for operation_rec, process_operation_fn in deployment_iterator:
            dao.session.commit()  # Update deployment and current operation status to RUNNING and next operations to PENDING
            if process_operation_fn:
                click.echo(
                    f"Operation {operation_rec.operation} is {operation_rec.state} {'for hosts: ' + operation_rec.host if operation_rec.host is not None else ''}"
                )
                if cluster_status_logs := process_operation_fn():
                    dao.session.add_all(cluster_status_logs)
            dao.session.commit()  # Update operation status to SUCCESS, FAILURE or HELD

        if deployment_iterator.deployment.state != DeploymentStateEnum.SUCCESS:
            raise click.ClickException("Deployment failed.")
        else:
            click.echo("Deployment finished with success.")
