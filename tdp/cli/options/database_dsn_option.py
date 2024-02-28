# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from click.decorators import FC


def database_dsn_option(func: FC) -> FC:
    """Click option that adds a database_dsn option to the command.

    The option is required and the value is a string.
    """
    return click.option(
        "--database-dsn",
        envvar="TDP_DATABASE_DSN",
        required=True,
        type=str,
        help=(
            "Database Data Source Name, in sqlalchemy driver form "
            "example: sqlite:////data/tdp.db or sqlite+pysqlite:////data/tdp.db. "
            "You might need to install the relevant driver to your installation (such "
            "as psycopg2 for postgresql)."
        ),
    )(func)
