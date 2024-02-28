# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import click
from click.decorators import FC

from tdp.core.collection import Collection
from tdp.core.collections import Collections


def _collections_from_paths(
    ctx: click.Context, param: click.Parameter, value: list[Path]
) -> Collections:
    """Transforms a list of paths into a Collections object.

    Args:
        ctx: Click context.
        param: Click parameter.
        value: List of collections paths.

    Returns:
        Collections object from the paths.

    Raises:
        click.BadParameter: If the value is empty.
    """
    if not value:
        raise click.BadParameter("cannot be empty", ctx=ctx, param=param)

    collections_list = [Collection.from_path(path) for path in value]
    collections = Collections.from_collection_list(collections_list)

    return collections


def collections_option(func: FC) -> FC:
    """Click option for the collection path.

    Takes multiple paths (required) and transforms them into a Collections object.
    """
    return click.option(
        "--collection-path",
        "collections",
        envvar="TDP_COLLECTION_PATH",
        required=True,
        multiple=True,
        type=click.Path(resolve_path=True, path_type=Path),
        callback=_collections_from_paths,
        help="Path to the collection. Can be used multiple times.",
        is_eager=True,  # This option is used by other options, so we need to parse it first
    )(func)
