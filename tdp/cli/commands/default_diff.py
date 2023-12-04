# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import difflib
import pprint
from collections import OrderedDict
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click

from tdp.cli.utils import collections, vars
from tdp.core.constants import DEFAULT_VARS_DIRECTORY_NAME
from tdp.core.variables import ClusterVariables, Variables, merge_hash

if TYPE_CHECKING:
    from tdp.core.collections import Collections


@click.command()
@click.argument("service", required=False)
@collections
@vars
def default_diff(collections: Collections, vars: Path, service: Optional[str] = None):
    """Difference between tdp_vars and defaults."""
    cluster_variables = ClusterVariables.get_cluster_variables(collections, vars)

    if service:
        service_diff(collections, cluster_variables[service])
    else:
        for service, service_variables in cluster_variables.items():
            service_diff(collections, service_variables)


def service_diff(collections, service):
    """Computes the difference between the default variables from a service, and the variables from your service variables inside your tdp_vars.

    Args:
        collections (Collections): Collections object.
        service (ServiceManager): Service to compare's manager.
    """
    # key: filename with extension, value: PosixPath(filepath)
    default_service_vars_paths = OrderedDict()
    for collection in collections.values():
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
        with Variables(tdp_vars_service_vars_filepath).open("r") as service_variables:
            service_varfile = service_variables.copy()

        default_service_varfile = {}
        for default_service_vars_filepath in default_service_vars_filepaths:
            with Variables(default_service_vars_filepath).open(
                "r"
            ) as default_variables:
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
    """Computes differences between 2 files, and outputs them.

    Args:
        service_name (str): Name of the service.
        filename (str): Name of the file to display.
        left_content (Iterator[str]): Content to compare from.
        right_content (Iterator[str]): Content to compare to.
        left_path (str): Filename to compare from, to use as contextual information.
        right_path (str): Filename to compare to, to use as contextual information.
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
