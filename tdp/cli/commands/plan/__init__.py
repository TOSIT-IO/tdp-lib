# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.commands.plan.dag import dag
from tdp.cli.commands.plan.edit import edit
from tdp.cli.commands.plan.ops import ops
from tdp.cli.commands.plan.reconfigure import reconfigure
from tdp.cli.commands.plan.resume import resume


@click.group()
def plan():
    """Generate a deployment plan."""
    pass


plan.add_command(dag)
plan.add_command(ops)
plan.add_command(edit)
plan.add_command(reconfigure)
plan.add_command(resume)
