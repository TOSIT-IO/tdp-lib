# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from .edit import edit
from .validate import validate


@click.group()
def vars():
    """Manage services and components configurations."""
    pass


vars.add_command(edit)
vars.add_command(validate)
