# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from click.decorators import FC


def rolling_interval_option(func: FC) -> FC:
    """Click option that adds a rolling_interval option to the command.

    The option is a flag.
    """
    return click.option(
        "-ri",
        "--rolling-interval",
        envvar="TDP_ROLLING_INTERVAL",
        type=int,
        help="Enable rolling restart with specific waiting time (in seconds) between component restart.",
    )(func)
