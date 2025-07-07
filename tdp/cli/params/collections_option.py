# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import click
from click.decorators import FC


def collections_option(func: FC) -> FC:
    """Click option for the collection path.

    Takes multiple paths (required) and transforms them into a Collections object.
    """

    from tdp.core.collections import Collections

    return click.option(
        "--collection-path",
        "collections",
        envvar="TDP_COLLECTION_PATH",
        required=True,
        multiple=True,
        type=click.Path(resolve_path=True, path_type=Path),
        callback=lambda _ctx, _param, value: Collections.from_collection_paths(value),
        help="Path to the collection. Can be used multiple times.",
        is_eager=True,  # This option is used by other options, so we need to parse it first
    )(func)
