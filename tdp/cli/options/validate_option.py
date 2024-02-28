# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from click.decorators import FC


def validate_option(func: FC) -> FC:
    """Click option that adds a validate option to the command.

    The option is a flag.
    """
    return click.option(
        "--validate/--no-validate",
        # TODO: set default to True when schema validation is fully implemented
        # default=True,
        help="Should the command validate service variables against defined JSON schemas.",
    )(func)
