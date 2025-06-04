# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pathlib

import click
from click.decorators import FC


def overrides_option(func: FC) -> FC:
    """Click option that adds an overrides option to the command."""
    return click.option(
        "--overrides",
        envvar="TDP_OVERRIDES",
        required=False,
        type=click.Path(exists=True, resolve_path=True, path_type=pathlib.Path),
        multiple=True,
        help="Path to TDP variables overrides. Can be used multiple times. Last one takes precedence.",
    )(func)
