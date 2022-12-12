# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click
from click.decorators import FC

from tdp.core.collection import Collection
from tdp.core.collections import Collections


def collection_paths_to_collections(ctx, param, value):
    if not value:
        raise click.BadParameter("cannot be empty", ctx=ctx, param=param)

    collections_list = [
        Collection.from_path(split) for split in value.split(os.pathsep)
    ]
    collections = Collections.from_collection_list(collections_list)

    return collections


def check_services_cleanliness(cluster_variables):
    unclean_services = [
        service_variables.name
        for service_variables in cluster_variables.values()
        if not service_variables.clean
    ]
    if unclean_services:
        for name in unclean_services:
            click.echo(
                f'"{name}" repository is not in a clean state.'
                " Check that all modifications are committed."
            )
        raise click.ClickException(
            "Some services are in a dirty state, commit your modifications."
        )


def collections(func: FC) -> FC:
    return click.option(
        "--collection-path",
        "collections",
        envvar="TDP_COLLECTION_PATH",
        required=True,
        callback=collection_paths_to_collections,  # transforms into Collections object
        help=f"List of paths separated by your os' path separator ({os.pathsep})",
    )(func)


def database_dsn(func: FC) -> FC:
    return click.option(
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
    )(func)


def dry(func: FC) -> FC:
    return click.option(
        "--dry", is_flag=True, help="Execute dag without running any action"
    )(func)


def run_directory(func: FC) -> FC:

    return click.option(
        "--run-directory",
        envvar="TDP_RUN_DIRECTORY",
        type=Path,
        help="Working directory where the executor is launched (`ansible-playbook` for Ansible)",
        required=True,
    )(func)


def vars(func: FC) -> FC:
    return click.option(
        "--vars",
        envvar="TDP_VARS",
        required=True,
        type=click.Path(resolve_path=True, path_type=Path),
        help="Path to the tdp vars",
    )(func)
