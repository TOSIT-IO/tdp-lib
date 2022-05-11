# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.commands.browse import browse
from tdp.cli.commands.dag import dag
from tdp.cli.commands.default_diff import default_diff
from tdp.cli.commands.deploy import deploy
from tdp.cli.commands.init import init
from tdp.cli.commands.nodes import nodes
from tdp.cli.commands.run import run
from tdp.cli.commands.service_versions import service_versions

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS)
@click.pass_context
def tdp(ctx):
    pass


tdp.add_command(browse)
tdp.add_command(dag)
tdp.add_command(default_diff)
tdp.add_command(deploy)
tdp.add_command(init)
tdp.add_command(nodes)
tdp.add_command(run)
tdp.add_command(service_versions)
