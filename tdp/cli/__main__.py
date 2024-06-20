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
from tdp.cli.logger import setup_logging

# Add `-h` shortcut to print the help for the whole cli.
# Click only uses `--help` by default.
CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


class CatchGroup(click.Group):
    """Catch exceptions and print them to stderr."""

    def __call__(self, *args, **kwargs):
        try:
            return self.main(*args, **kwargs)

        except Exception as e:
            click.echo(f"Error: {e}", err=True)


def load_env(ctx: click.Context, param: click.Parameter, value: Path) -> Optional[Path]:
    """Click callback to load the environment file."""
    if value.exists():
        load_dotenv(value)
        return value
    else:
        logging.warning(f"Environment file {value} does not exist.")


@click.group("tdp", context_settings=CONTEXT_SETTINGS, cls=CatchGroup)
@click.option(
    "--env",
    default=".env",
    envvar="TDP_ENV",
    callback=load_env,
    type=Path,
    help="Path to environment configuration file.",
)
@click.option(
    "-d",
    "--debug",
    envvar="TDP_LOG_CONF_FILE",
    help="Set the config file for the logging.",
)
def cli(env: Path, debug: str):
    setup_logging(debug)
    logging.info("Logging is configured.")


def main():
    cli.add_command(browse)
    cli.add_command(dag)
    cli.add_command(default_diff)
    cli.add_command(deploy)
    cli.add_command(init)
    cli.add_command(ops)
    cli.add_command(plan)
    cli.add_command(status)
    cli.add_command(vars)

    cli()


if __name__ == "__main__":
    main()
