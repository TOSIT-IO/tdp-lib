import click
from dotenv import load_dotenv

from tdp.cli.utils import env

from .dag import dag
from .reconfigure import reconfigure
from .resume import resume
from .run import run

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


@click.group(context_settings=CONTEXT_SETTINGS, invoke_without_command=True)
@env
def plan(env):
    load_dotenv(env)


plan.add_command(run)
plan.add_command(resume)
plan.add_command(reconfigure)
plan.add_command(dag)
