# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from click.decorators import FC


def preview_option(func: FC) -> FC:
    """Click option that adds a preview option to the command.

    The option is a flag.
    """
    return click.option(
        "--preview",
        is_flag=True,
        help="Preview the plan without running any action.",
    )(func)
