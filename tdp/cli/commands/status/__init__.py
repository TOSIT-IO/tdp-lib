# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from .edit import edit
from .generate_stales import generate_stales
from .show import show


@click.group()
def status() -> None:
    """Manage the status of the cluster."""
    pass


status.add_command(edit)
status.add_command(generate_stales)
status.add_command(show)
