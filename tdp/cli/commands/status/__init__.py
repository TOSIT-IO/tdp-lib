# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.commands.status.edit import edit
from tdp.cli.commands.status.show import show


@click.group()
def status() -> None:
    """Manage the status of the cluster."""
    pass


status.add_command(edit)
status.add_command(show)
