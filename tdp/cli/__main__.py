# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from dotenv import load_dotenv

from tdp.cli.utils import env
from tdp.cli.commands.browse import browse
from tdp.cli.commands.dag import dag
from tdp.cli.commands.default_diff import default_diff
from tdp.cli.commands.init import init
from tdp.cli.commands.nodes import nodes
from tdp.cli.commands.playbooks import playbooks
from tdp.cli.commands.service_versions import service_versions
from tdp.cli.commands.validate import validate
from tdp.cli.commands.plan import plan

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
@env
def tdp(env):
    load_dotenv(env)


tdp.add_command(browse)
tdp.add_command(dag)
tdp.add_command(default_diff)
tdp.add_command(init)
tdp.add_command(nodes)
tdp.add_command(playbooks)
tdp.add_command(service_versions)
tdp.add_command(validate)

tdp.add_command(plan)
