# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import click

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Optional

    from click.decorators import FC


def force_option(func: FC) -> FC:
    """Add the `--force` option to a Click command.

    The option is a flag.
    """
    return click.option(
        "--force",
        envvar="TDP_FORCE_PLAN_OVERRIDE",
        is_flag=True,
        help="Force overriding an existing deployment plan.",
    )(func)


def preview_option(func: FC) -> FC:
    """Add the `--preview` option to a Click command.

    The option is a flag.
    """
    return click.option(
        "--preview",
        is_flag=True,
        help="Preview the plan without running any action.",
    )(func)


def rolling_interval_option(func: FC) -> FC:
    """Add the `--rolling-interval` option to a Click command.

    The option is a flag.
    """
    return click.option(
        "-ri",
        "--rolling-interval",
        envvar="TDP_ROLLING_INTERVAL",
        type=int,
        help="Enable rolling restart with specific waiting time (in seconds) between component restart.",
    )(func)


def component_argument_option(func: FC) -> FC:
    """Add the COMPONENT argument to a Click command."""

    return click.argument("component", nargs=1, required=False)(func)


def service_argument_option(
    func: Optional[FC] = None, *, required=False
) -> Callable[[FC], FC]:
    """Add the SERVICE argument to a Click command."""

    def decorator(fn: FC) -> FC:
        return click.argument("service", nargs=1, required=required)(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)


def collections_option(func: FC) -> FC:
    """Add the `--collection-path` option to a Click command.

    Takes multiple paths (required) and transforms them into a Collections object.
    Available as "collections" in the command context.
    """

    def _create_collections_callback(
        _ctx: click.Context, _param: click.Parameter, value
    ):
        """Click callback that creates a Collections object."""
        from tdp.core.collections import Collections

        return Collections.from_collection_paths(value)

    return click.option(
        "--collection-path",
        "collections",
        envvar="TDP_COLLECTION_PATH",
        required=True,
        multiple=True,
        type=click.Path(resolve_path=True, path_type=pathlib.Path),
        callback=_create_collections_callback,
        help="Path to the collection. Can be used multiple times.",
    )(func)


def database_dsn_option(func: FC) -> FC:
    """Add the `--database-dsn` option to a Click command.

    Return a SQLAlchemy Engine instance, available as "db_engine" in the command context.
    """

    def _get_engine_callback(_ctx: click.Context, _param: click.Parameter, value):
        """Click callback that returns a SQLAlchemy Engine instance."""
        from tdp.core.db import get_engine

        return get_engine(value)

    return click.option(
        "db_engine",
        "--database-dsn",
        envvar="TDP_DATABASE_DSN",
        required=True,
        type=str,
        callback=_get_engine_callback,
        help=(
            "Database Data Source Name, in sqlalchemy driver form "
            "example: sqlite:////data/tdp.db or sqlite+pysqlite:////data/tdp.db. "
            "You might need to install the relevant driver to your installation (such "
            "as psycopg2 for postgresql)."
        ),
    )(func)


def hosts_option(func: Optional[FC] = None, *, help: str) -> Callable[[FC], FC]:
    """Add the `--host` option to a Click command.

    Takes multiple hosts and transforms them into a tuple of strings. Available as
    "hosts" in the command context.

    Args:
        help: The help text for the option.
    """

    def decorator(fn: FC) -> FC:
        return click.option(
            "--host",
            "hosts",
            envvar="TDP_HOSTS",
            type=str,
            multiple=True,
            help=help,
        )(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)


def conf_option(func: FC) -> FC:
    """Add the `--conf` option to a Click command."""
    return click.option(
        "--conf",
        envvar="TDP_CONF",
        required=False,
        type=click.Path(exists=True, resolve_path=True, path_type=pathlib.Path),
        multiple=True,
        help="Path to the user variable configuration directory. Can be used multiple times. Last one takes precedence.",
    )(func)


def validate_option(func: FC) -> FC:
    """Add the `--validate` option to a Click command.

    The option is a flag.
    """
    return click.option(
        "--validate/--no-validate",
        default=False,  # TODO: set to True when schema validation is fully implemented
        help="Should the command validate service variables against defined JSON schemas.",
    )(func)


def vars_option(func: Optional[FC] = None, *, exists=True) -> Callable[[FC], FC]:
    """Add the `--vars` option to a Click command.

    Args:
        exists: If True, the path must exist. If False, the path can be created.
    """

    def decorator(fn: FC) -> FC:
        return click.option(
            "--vars",
            envvar="TDP_VARS",
            required=True,
            type=click.Path(
                resolve_path=True,
                path_type=pathlib.Path,
                exists=exists,
                file_okay=False,
                dir_okay=True,
                writable=True,
            ),
            help="Path to the TDP variables.",
        )(fn)

    # Checks if the decorator was used without parentheses.
    if func is None:
        return decorator
    else:
        return decorator(func)
