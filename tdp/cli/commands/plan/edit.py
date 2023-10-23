# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import re
import tempfile
from contextlib import contextmanager
from typing import Optional

import click

from tdp.cli.queries import get_planned_deployment
from tdp.cli.session import get_session
from tdp.cli.utils import collections, database_dsn
from tdp.core.models.deployment_model import DeploymentModel

logger = logging.getLogger("tdp").getChild("edit")


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


def _parse_line(line: str) -> tuple[str, Optional[str], Optional[list[str]]]:
    """Parses a line which contains an operation, and eventually a host and extra vars.

    Args:
        line: Line to be parsed.

    Returns:
        Operation, host and extra vars.
    """
    parsed_line = re.match(
        r"^(.*?)( on .*?){0,1}( ?with .*?){0,1}( ?on .*?){0,1}$", line
    )

    if parsed_line is None:
        raise ValueError(
            "Error on line '"
            + line
            + "': it must be 'OPERATION [on HOST] [with EXTRA_VARS[,EXTRA_VARS]].'"
        )

    if parsed_line.group(1).split(" ")[0] == "":
        raise ValueError("Error on line '" + line + "': it must contain an operation.")

    if len(parsed_line.group(1).strip().split(" ")) > 1:
        raise ValueError("Error on line '" + line + "': only 1 operation is allowed.")

    if parsed_line.group(2) is not None and parsed_line.group(4) is not None:
        raise ValueError(
            "Error on line '" + line + "': only 1 host is allowed in a line."
        )

    operation = parsed_line.group(1).strip()

    # Get the host and test if it is declared
    if parsed_line.group(2) is not None:
        host = parsed_line.group(2).split(" on ")[1]
        if host == "":
            raise ValueError(
                "Error on line '" + line + "': host is required after 'on' keyword."
            )
    elif parsed_line.group(4) is not None:
        host = parsed_line.group(4).split(" on ")[1]
        if host == "":
            raise ValueError(
                "Error on line '" + line + "': host is required after 'on' keyword."
            )
    else:
        host = None

    # Get the extra vars and test if they are declared
    if parsed_line.group(3) is not None:
        extra_vars = parsed_line.group(3).split(" with ")[1]
        if extra_vars == "":
            raise ValueError("Extra vars are required after 'with' keyword.")
        extra_vars = extra_vars.split(",")
        extra_vars = [item.strip() for item in extra_vars]
    else:
        extra_vars = None

    return (operation, host, extra_vars)


def _parse_file(file_name) -> list[tuple[str, Optional[str], Optional[list[str]]]]:
    """Parses a file which contains operations, hosts and extra vars."""
    file_content = file_name.read()
    return [
        _parse_line(line)
        for line in file_content.split("\n")
        if line and not line.startswith("#")
    ]


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
    collections,
    database_dsn,
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
                        new_operations_hosts_vars = _parse_file(file)

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
