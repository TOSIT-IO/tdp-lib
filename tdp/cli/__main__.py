# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.commands.browse import browse
from tdp.cli.commands.dag import dag
from tdp.cli.commands.default_diff import default_diff
from tdp.cli.commands.deploy import deploy
from tdp.cli.commands.init import init
from tdp.cli.commands.nodes import nodes
from tdp.cli.commands.service_versions import service_versions
from tdp.core.dag import Dag


@click.group()
@click.pass_context
def tdp(ctx):
    ctx.obj = Dag()


tdp.add_command(browse)
tdp.add_command(dag)
tdp.add_command(default_diff)
tdp.add_command(deploy)
tdp.add_command(init)
tdp.add_command(nodes)
tdp.add_command(service_versions)
