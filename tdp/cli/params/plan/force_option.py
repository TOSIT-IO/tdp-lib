# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from click.decorators import FC


def force_option(func: FC) -> FC:
    """Click option that adds a force option to the command.

    The option is a flag.
    """
    return click.option(
        "--force",
        envvar="TDP_FORCE_PLAN_OVERRIDE",
        is_flag=True,
        help="Force overriding an existing deployment plan.",
    )(func)
