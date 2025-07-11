# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pathlib

import click
from click.decorators import FC


def conf_option(func: FC) -> FC:
    """Add the `--conf` option to a Click command."""
    return click.option(
        "--conf",
        envvar="TDP_CONF",
        required=False,
        type=click.Path(exists=True, resolve_path=True, path_type=pathlib.Path),
        multiple=True,
        help="Path to the user variable configuration directory. Can be used multiple times. Last one takes precedence.",
    )(func)
