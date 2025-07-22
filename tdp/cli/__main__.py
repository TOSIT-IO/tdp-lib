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
from tdp.cli.commands.ops import ops
from tdp.cli.commands.plan import plan
from tdp.cli.commands.status import status
from tdp.cli.commands.vars import vars
from tdp.cli.logger import DEFAULT_LOG_LEVEL, get_early_logger, setup_logging

# Set up early logging before any other operations
logger = get_early_logger(__name__)

# Add `-h` shortcut to print the help for the whole cli.
# Click only uses `--help` by default.
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


def load_env(ctx: click.Context, param: click.Parameter, value: Path) -> Optional[Path]:
    """Click callback to load the environment file."""
    if value.exists():
        load_dotenv(value)
        logger.info(f"Loaded environment from {value}")
        return value
    else:
        logger.warning(f"Environment file {value} does not exist.")


def logging_callback(ctx: click.Context, param: click.Parameter, value: str) -> None:
    """Click callback to set up logging."""
    setup_logging(value)
    logger.info(f"Logging level set to {value}")


@click.group("tdp", context_settings=CONTEXT_SETTINGS)
@click.option(
    "--env",
    default=".env",
    envvar="TDP_ENV",
    callback=load_env,
    type=Path,
    help="Path to environment configuration file.",
    expose_value=False,
    is_eager=True,
)
@click.option(
    "--log-level",
    default=logging.getLevelName(DEFAULT_LOG_LEVEL),
    envvar="TDP_LOG_LEVEL",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
    callback=logging_callback,
    help="Set the level of log output.",
    show_default=True,
    expose_value=False,
    is_eager=True,
)
def cli():
    pass


cli.add_command(browse)
cli.add_command(dag)
cli.add_command(default_diff)
cli.add_command(deploy)
cli.add_command(init)
cli.add_command(ops)
cli.add_command(plan)
cli.add_command(status)
cli.add_command(vars)


if __name__ == "__main__":
    cli()
