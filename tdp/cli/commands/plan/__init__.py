# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from .dag import dag
from .reconfigure import reconfigure
from .resume import resume
from .run import run

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(
    context_settings=CONTEXT_SETTINGS,
    invoke_without_command=True,
    short_help="Generate a deployment plan.",
)
def plan():
    pass


plan.add_command(run)
plan.add_command(resume)
plan.add_command(reconfigure)
plan.add_command(dag)
