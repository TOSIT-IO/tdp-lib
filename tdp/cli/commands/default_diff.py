# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import difflib
import pprint
from pathlib import Path

import click
import yaml

from tdp.cli.context import pass_dag
from tdp.core.service_manager import ServiceManager


@click.command(short_help="Difference between tdp_vars and defaults")
@click.argument("service", required=False)
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    type=Path,
    help="Path to tdp-collection",
)
@click.option(
    "--vars", envvar="TDP_VARS", required=True, type=Path, help="Path to the tdp vars"
)
@pass_dag
def default_diff(dag, service, collection_path, vars):
    service_managers = ServiceManager.get_service_managers(dag, vars)
    collection_default_vars = collection_path / "tdp_vars_defaults"

    if service:
        service_diff(collection_default_vars, service_managers[service])
    else:
        for service in dag.services:
            service_diff(collection_default_vars, service_managers[service])


def service_diff(collection_default_vars, service):
    """computes the difference between the default variables from a service, and the variables from your service variables inside your tdp_vars

    Args:
        collection_default_vars (Path): default vars path
        service (ServiceManager): service to compare's manager
    """
    default_service_vars = collection_default_vars / service.name
    # key: filename with extension, value: PosixPath(filepath)
    default_service_vars_paths = {
        path.name: path for path in default_service_vars.glob("*.yml")
    }
    for (
        default_service_vars_filename,
        default_service_vars_filepath,
    ) in default_service_vars_paths.items():
        tdp_vars_service_vars_filepath = service.path / default_service_vars_filename
        if not tdp_vars_service_vars_filepath.exists():
            click.echo(
                f"{service.name}: {default_service_vars_filename}\n"
                + click.style(
                    f"{tdp_vars_service_vars_filepath} does not exist", fg="red"
                )
            )
            continue

        with default_service_vars_filepath.open(
            "r"
        ) as default_service_varfile, tdp_vars_service_vars_filepath.open(
            "r"
        ) as service_varfile:
            default_service_varfile_content = pprint.pformat(
                yaml.safe_load(default_service_varfile)
            ).splitlines()
            service_varfile_content = pprint.pformat(
                yaml.safe_load(service_varfile)
            ).splitlines()
            # left_path = tdp_vars_defaults/{service}/{filename}
            left_path = str(
                default_service_vars_filepath.relative_to(
                    collection_default_vars.parent
                )
            )
            # right_path = {your_tdp_vars}/{service}/{filename}
            right_path = str(
                tdp_vars_service_vars_filepath.relative_to(service.path.parent.parent)
            )
            compute_and_print_difference(
                service_name=service.name,
                left_content=default_service_varfile_content,
                right_content=service_varfile_content,
                left_path=left_path,
                right_path=right_path,
                filename=default_service_vars_filename,
            )


def compute_and_print_difference(
    service_name, filename, left_content, right_content, left_path, right_path
):
    """computes differences between 2 files, and outputs them.

    Args:
        service_name (str): name of the service
        filename (str): name of the file to display
        left_content (Iterator[str]): content to compare from
        right_content (Iterator[str]): content to compare to
        left_path (str): filename to compare from, to use as contextual information
        right_path (str): filename to compare to, to use as contextual information
    """
    differences = difflib.context_diff(
        left_content,
        right_content,
        fromfile=left_path,
        tofile=right_path,
        n=1,
    )
    click.echo(
        f"{service_name}: {filename}\n"
        + ("\n".join(color_line(line) for line in differences) or "None")
    )


def color_line(line: str):
    if line.startswith("! "):
        return click.style(line, fg="yellow")
    if line.startswith("- "):
        return click.style(line, fg="red")
    if line.startswith("+ "):
        return click.style(line, fg="green")

    return line
