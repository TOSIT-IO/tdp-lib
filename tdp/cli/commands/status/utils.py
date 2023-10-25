# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import click
from click.decorators import FC

from tdp.cli.utils import (
    collections,
    database_dsn,
    print_table,
    validate,
    vars,
)
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.cluster_status import SCHStatus
    from tdp.core.collections import Collections


def _check_service(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[str]:
    """Click callback that check if the service exists."""
    collections: Collections = ctx.params["collections"]
    vars: Path = ctx.params["vars"]
    # TODO: would be nice if services can be retrieved from the collections
    cluster_variables = ClusterVariables.get_cluster_variables(
        collections=collections, tdp_vars=vars, validate=False
    )
    if value and value not in cluster_variables.keys():
        raise click.UsageError(f"Service '{value}' does not exists.")
    return value


def _check_component(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[str]:
    """Click callback that check if the component exists."""
    collections: Collections = ctx.params["collections"]
    service: str = ctx.params["service"]
    if value and value not in [
        sc_name.component_name
        for sc_name in collections.get_components_from_service(service)
    ]:
        raise click.UsageError(
            f"Component '{value}' does not exists in service '{service}'."
        )
    return value


def _common_status_options(func: FC) -> FC:
    """Add common status options to the command."""
    for option in reversed(
        [
            click.argument("service", nargs=1, required=False, callback=_check_service),
            click.argument(
                "component", nargs=1, required=False, callback=_check_component
            ),
            collections,
            database_dsn,
            validate,
            vars,
        ]
    ):
        func = option(func)  # type: ignore
    return func


def _print_sch_status_logs(sch_status: Iterable[SCHStatus]) -> None:
    print_table(
        [status.to_dict(filter_out=["id", "timestamp"]) for status in sch_status],
    )
