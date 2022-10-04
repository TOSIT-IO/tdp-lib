# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os

import click

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
