import click
from tdp.cli.run.commands.deploy import deploy

from tdp.core.dag import Dag

from tdp.cli.run.commands.browse import browse
from tdp.cli.run.commands.default_diff import default_diff
from tdp.cli.run.commands.init import init
from tdp.cli.run.commands.nodes import nodes
from tdp.cli.run.commands.service_versions import service_versions


@click.group()
@click.pass_context
def tdp(ctx):
    ctx.obj = Dag()


tdp.add_command(browse)
tdp.add_command(default_diff)
tdp.add_command(deploy)
tdp.add_command(init)
tdp.add_command(nodes)
tdp.add_command(service_versions)
