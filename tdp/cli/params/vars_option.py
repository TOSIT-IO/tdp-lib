# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Callable
from pathlib import Path
from typing import Optional

import click
from click.decorators import FC


def vars_option(func: Optional[FC] = None, *, exists=True) -> Callable[[FC], FC]:
    """Click option for the TDP variables.

    The option is required and takes a path to the TDP variables.
    """

    def decorator(fn: FC) -> FC:
        return click.option(
            "--vars",
            envvar="TDP_VARS",
            required=True,
            type=click.Path(
                resolve_path=True,
                path_type=Path,
                exists=exists,
                file_okay=False,
                dir_okay=True,
                writable=True,
            ),
            help="Path to the TDP variables.",
            is_eager=True,  # This option is used by other options, so we need to parse it first
        )(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)
