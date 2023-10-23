# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.commands.vars.edit import edit


@click.group()
def vars():
    """Manage services and components configurations."""
    pass


vars.add_command(edit)
