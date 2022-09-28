# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import difflib
import os
import pprint
from collections import OrderedDict
from pathlib import Path

import click

from tdp.cli.utils import collection_paths
from tdp.core.collection import DEFAULT_VARS_DIRECTORY_NAME
from tdp.core.dag import Dag
from tdp.core.service_manager import ServiceManager
from tdp.core.variables import Variables, merge_hash


@click.command(short_help="Difference between tdp_vars and defaults")
@click.argument("service", required=False)
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    callback=collection_paths,  # transforms list of path into Collections
    help=f"List of paths separated by your os' path separator ({os.pathsep})",
)
@click.option(
    "--vars",
    envvar="TDP_VARS",
    required=True,
    type=click.Path(resolve_path=True, path_type=Path),
    help="Path to the tdp vars",
)
def default_diff(service, collection_path, vars):
    if not vars.exists():
        raise click.BadParameter(f"{vars} does not exist")
    dag = Dag(collection_path)
    service_managers = ServiceManager.get_service_managers(dag, vars)

    if service:
        service_diff(service_managers[service])
    else:
        for service in dag.services:
            service_diff(service_managers[service])


def service_diff(service):
    """computes the difference between the default variables from a service, and the variables from your service variables inside your tdp_vars

    Args:
        collection_default_vars (Path): default vars path
        service (ServiceManager): service to compare's manager
    """
    # key: filename with extension, value: PosixPath(filepath)
    default_service_vars_paths = OrderedDict()
    for collection in service.dag.collections.values():
        default_vars = collection.get_service_default_vars(service.name)
        if not default_vars:
            continue
        for name, path in default_vars:
            default_service_vars_paths.setdefault(name, []).append(path)

    for (
        default_service_vars_filename,
        default_service_vars_filepaths,
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
        service_varfile = {}
        with Variables(tdp_vars_service_vars_filepath).open() as service_variables:
            service_varfile = service_variables.copy()

        default_service_varfile = {}
        for default_service_vars_filepath in default_service_vars_filepaths:
            with Variables(default_service_vars_filepath).open() as default_variables:
                default_service_varfile = merge_hash(
                    default_service_varfile, default_variables
                )

        service_varfile_content = pprint.pformat(service_varfile).splitlines()
        default_service_varfile_content = pprint.pformat(
            default_service_varfile
        ).splitlines()

        # left_path = tdp_vars_defaults/{service}/{filename}
        # multiple paths if merged from multiple collections
        paths = [
            str(
                filepath.relative_to(find_parent(filepath, DEFAULT_VARS_DIRECTORY_NAME))
            )
            for filepath in default_service_vars_filepaths
        ]
        context = "" if len(paths) < 2 else " <-- merged"
        left_path = ",".join(paths) + context

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


def find_parent(path, name):
    parent_path = path
    while parent_path.name != name and parent_path is not None:
        parent_path = parent_path.parent
    return parent_path.parent.parent
