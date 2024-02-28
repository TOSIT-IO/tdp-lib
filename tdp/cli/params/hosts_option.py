# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Callable
from typing import Optional

import click
from click.decorators import FC


def hosts_option(func: Optional[FC] = None, *, help: str) -> Callable[[FC], FC]:
    """Click option for the hosts.

    Takes multiple hosts and transforms them into a tuple of strings.
    """

    def decorator(fn: FC) -> FC:
        return click.option(
            "--host",
            "hosts",
            envvar="TDP_HOSTS",
            type=str,
            multiple=True,
            help=help,
        )(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)
