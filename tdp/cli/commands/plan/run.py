# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.queries import get_planned_deployment_log
from tdp.cli.session import get_session
from tdp.cli.utils import (
    collections,
    database_dsn,
    preview,
    print_deployment,
    rolling_interval,
)
from tdp.core.models import DeploymentLog


@click.command(short_help="Run single TDP operation.")
@click.argument("operation_names", nargs=-1, required=True)
@click.option(
    "--host",
    envvar="TDP_HOSTS",
    type=str,
    multiple=True,
    help="Hosts where operations are launched. Can be used multiple times.",
)
@click.option(
    "-e",
    "--extra-vars",
    envvar="TDP_EXTRA_VARS",
    type=str,
    multiple=True,
    help="Extra vars for operations (forwarded to ansible as is). Can be used multiple times.",
)
@collections
@database_dsn
@preview
@rolling_interval
def run(
    operation_names,
    host,
    extra_vars,
    collections,
    database_dsn,
    preview,
    rolling_interval,
):
    try:
        click.echo(
            f"Creating a deployment plan to run {len(operation_names)} operation(s)."
        )
        deployment_log = DeploymentLog.from_operations(
            collections, operation_names, host, extra_vars, rolling_interval
        )
        if preview:
            print_deployment(deployment_log)
            return
        with get_session(database_dsn, commit_on_exit=True) as session:
            planned_deployment_log = get_planned_deployment_log(session)
            if planned_deployment_log:
                deployment_log.id = planned_deployment_log.id
            session.merge(deployment_log)
        click.echo("Deployment plan successfully created.")

    except Exception as e:
        raise click.ClickException(e)