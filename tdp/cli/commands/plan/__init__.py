# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.commands.plan.dag import dag
from tdp.cli.commands.plan.edit import edit
from tdp.cli.commands.plan.ops import ops
from tdp.cli.commands.plan.reconfigure import reconfigure
from tdp.cli.commands.plan.resume import resume

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(
    context_settings=CONTEXT_SETTINGS,
    short_help="Generate a deployment plan.",
)
def plan():
    pass


plan.add_command(dag)
plan.add_command(ops)
plan.add_command(edit)
plan.add_command(reconfigure)
plan.add_command(resume)
