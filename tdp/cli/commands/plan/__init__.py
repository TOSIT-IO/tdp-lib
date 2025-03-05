# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from .dag import dag
from .edit import edit
from .import_file import import_file
from .ops import ops
from .reconfigure import reconfigure
from .resume import resume


@click.group()
def plan():
    """Generate a deployment plan."""
    pass


plan.add_command(dag)
plan.add_command(edit)
plan.add_command(import_file)
plan.add_command(ops)
plan.add_command(reconfigure)
plan.add_command(resume)
