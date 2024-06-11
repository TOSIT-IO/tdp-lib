# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Callable
from typing import Optional

import click
from click.decorators import FC
from sqlalchemy import Engine, create_engine


def _engine_from_dsn(ctx: click.Context, param: click.Parameter, value: str) -> Engine:
    """Transforms a database DSN into an engine.

    Args:
        ctx: Click context.
        param: Click parameter.
        value: Database DSN.

    Returns:
        Engine from the DSN.
    """
    return create_engine(value, future=True)


def database_dsn_option(
    func: Optional[FC] = None, create_engine: bool = True
) -> Callable[[FC], FC]:
    """Click option that adds a database_dsn option to the command.

    It returns a sqlalchemy engine if create_engine is True, otherwise it returns the database_dsn.
    """

    def decorator(fn: FC) -> FC:
        return click.option(
            "db_engine",
            "--database-dsn",
            envvar="TDP_DATABASE_DSN",
            required=True,
            type=str,
            callback=_engine_from_dsn if create_engine else None,
            help=(
                "Database Data Source Name, in sqlalchemy driver form "
                "example: sqlite:////data/tdp.db or sqlite+pysqlite:////data/tdp.db. "
                "You might need to install the relevant driver to your installation (such "
                "as psycopg2 for postgresql)."
            ),
        )(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)
