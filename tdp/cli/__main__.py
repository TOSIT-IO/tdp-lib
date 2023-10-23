# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from typing import Optional

import click
from dotenv import load_dotenv

from tdp.cli.commands.browse import browse
from tdp.cli.commands.dag import dag
from tdp.cli.commands.default_diff import default_diff
from tdp.cli.commands.deploy import deploy
from tdp.cli.commands.init import init
from tdp.cli.commands.operations import operations
from tdp.cli.commands.plan import plan
from tdp.cli.commands.playbooks import playbooks
from tdp.cli.commands.status import status
from tdp.cli.commands.validate import validate
from tdp.cli.commands.vars import vars
from tdp.cli.logger import setup_logging

# Add `-h` shortcut to print the help for the whole cli.
# Click only uses `--help` by default.
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


def load_env(ctx: click.Context, param: click.Parameter, value: Path) -> Optional[Path]:
    """Click callback to load the environment file."""
    if value.exists():
        load_dotenv(value)
        return value
    else:
        logging.warning(f"Environment file {value} does not exist.")


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option(
    "--env",
    default=".env",
    envvar="TDP_ENV",
    callback=load_env,
    type=Path,
    help="Path to environment configuration file",
)
@click.option(
    "--log-level",
    default="INFO",
    envvar="TDP_LOG_LEVEL",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    help="Set the level of log output.",
)
def tdp(env: Path, log_level: str):
    setup_logging(log_level)
    logging.info("Logging is configured.")


tdp.add_command(browse)
tdp.add_command(dag)
tdp.add_command(default_diff)
tdp.add_command(deploy)
tdp.add_command(init)
tdp.add_command(operations)
tdp.add_command(plan)
tdp.add_command(playbooks)
tdp.add_command(status)
tdp.add_command(validate)
tdp.add_command(vars)
