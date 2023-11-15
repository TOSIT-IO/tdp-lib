# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import os
import tempfile
from contextlib import contextmanager
from typing import TYPE_CHECKING

import click

from tdp.cli.queries import get_planned_deployment
from tdp.cli.session import get_session
from tdp.cli.utils import collections, database_dsn, parse_file
from tdp.core.models.deployment_model import DeploymentModel

if TYPE_CHECKING:
    from tdp.core.collections import Collections

logger = logging.getLogger(__name__)


def _get_header_message(deployment_id: int, temp_file_name: str) -> str:
    """Returns the header message to be displayed in the temporary file."""
    return f"""\
# ------------------- DEPLOYMENT PLAN EDITOR --------------------------
# Modify this file to edit the deployment plan.
# Lines beginning with '#' are comments and will be ignored.
#
# Current planned deployment ID: {deployment_id}
# Temporary file: {temp_file_name}
#
# Execution order:
# - Operations are executed in the order they appear below.
# - You can add, remove, or rearrange operations.
# - Use `tdp nodes` command to view a list of available operations.
#
# Operations options:
# - Specify a host with the `on` keyword.
#   Example: hdfs_datanode_config on worker_02
# - Add extra variables using the `with` keyword and separate with `,`.
#   Example: wait_sleep with rolling-interval=5, mock-arg=example
# ------------------- DEPLOYMENT PLAN EDITOR --------------------------
"""


@contextmanager
def _managed_temp_file(**kwargs):
    """Creates a temporary file and deletes it when the context is exited.

    Args:
        **kwargs: Arguments to be passed to tempfile.NamedTemporaryFile.

    Yield:
        Temporary file.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, **kwargs)
    try:
        yield tmp
    finally:
        tmp.close()
        os.unlink(tmp.name)


@click.command()
@collections
@database_dsn
def edit(
    collections: Collections,
    database_dsn: str,
):
    """Edit the planned deployment."""
    with get_session(database_dsn, commit_on_exit=True) as session:
        planned_deployment = get_planned_deployment(session)
        if planned_deployment is None:
            raise click.ClickException(
                "No planned deployment found, please run `tdp plan` first."
            )

        operation_list = [
            (operation.operation, operation.host, operation.extra_vars)
            for operation in planned_deployment.operations
        ]

        operation_lines = [
            operation
            + (" on " + host if host else "")
            + (" with " + ",".join(extra_vars) if extra_vars else "")
            for operation, host, extra_vars in operation_list
        ]

        with _managed_temp_file(mode="w", suffix=".csv") as temp_file:
            header_text = _get_header_message(planned_deployment.id, temp_file.name)

            temp_file.write(header_text)
            temp_file.write("\n".join(operation_lines))
            temp_file.close()

            click.echo(
                f"Current plan id: {planned_deployment.id}\nTemporary file: {temp_file.name}"
            )

            # Loop until plan has no error or if user wants to stop editing the plan
            while True:
                click.edit(
                    filename=temp_file.name,
                )

                value: str = click.prompt(
                    "Press any key when done editing ('e' to edit the plan again)",
                    type=str,
                    prompt_suffix="",
                    default="continue",
                    show_default=False,
                )
                if value.lower() == "e":
                    continue

                with open(temp_file.name) as file:
                    try:
                        # Remove empty elements and comments
                        # and get the operations, hosts and extra vars in a list
                        new_operations_hosts_vars = parse_file(file)

                        if new_operations_hosts_vars == operation_list:
                            raise click.ClickException("Plan was not modified.")

                        if not new_operations_hosts_vars:
                            raise click.ClickException("Plan must not be empty.")

                        deployment = DeploymentModel.from_operations_hosts_vars(
                            collections, new_operations_hosts_vars
                        )

                        deployment.id = planned_deployment.id
                        session.merge(deployment)
                        session.commit()
                        click.echo("Deployment plan successfully modified.")
                        break
                    except Exception as e:
                        logger.error(str(e))
                        value: str = click.prompt(
                            "Press any key to edit the plan again ('q' to quit)",
                            type=str,
                            prompt_suffix="",
                            default="continue",
                            show_default=False,
                        )
                        if value.lower() == "q":
                            click.echo("Abort plan edition")
                            break
