# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pathlib
from typing import Optional

import click
from click.decorators import FC


def service_argument_option(func: FC) -> FC:
    """Add service argument to the command."""

    from tdp.core.collections import Collections
    from tdp.core.variables.cluster_variables import ClusterVariables

    def _check_service(
        ctx: click.Context, param: click.Parameter, value: Optional[str]
    ) -> Optional[str]:
        """Click callback that check if the service exists."""
        collections: Collections = ctx.params["collections"]
        vars: pathlib.Path = ctx.params["vars"]
        # TODO: would be nice if services can be retrieved from the collections
        cluster_variables = ClusterVariables.get_cluster_variables(
            collections=collections, tdp_vars=vars, validate=False
        )
        if value and value not in cluster_variables.keys():
            raise click.UsageError(f"Service '{value}' does not exists.")
        return value

    return click.argument("service", nargs=1, required=False, callback=_check_service)(
        func
    )
