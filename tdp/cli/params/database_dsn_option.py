# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click
from click.decorators import FC


def database_dsn_option(func: FC) -> FC:
    """Click option that adds a database_dsn option to the command."""

    from sqlalchemy import create_engine

    return click.option(
        "db_engine",
        "--database-dsn",
        envvar="TDP_DATABASE_DSN",
        required=True,
        type=str,
        callback=lambda _ctx, _param, value: create_engine(value, future=True),
        help=(
            "Database Data Source Name, in sqlalchemy driver form "
            "example: sqlite:////data/tdp.db or sqlite+pysqlite:////data/tdp.db. "
            "You might need to install the relevant driver to your installation (such "
            "as psycopg2 for postgresql)."
        ),
    )(func)
