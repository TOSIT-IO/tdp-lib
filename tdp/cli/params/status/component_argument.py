# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import click
from click.decorators import FC

from tdp.core.entities.hostable_entity_name import ServiceComponentName, ServiceName

if TYPE_CHECKING:
    from tdp.core.collections import Collections


def _check_component(
    ctx: click.Context, param: click.Parameter, value: Optional[str]
) -> Optional[ServiceComponentName]:
    """Click callback that check if the component exists."""
    collections: Collections = ctx.params["collections"]
    service: ServiceName = ctx.params["service"]
    if not value:
        return
    if value not in [
        sc_name.component
        for sc_name in collections.get_components_from_service(service.name)
    ]:
        raise click.UsageError(
            f"Component '{value}' does not exists in service '{service}'."
        )
    return ServiceComponentName(service.name, value)


def component_argument_option(func: FC) -> FC:
    """Add component argument to the command."""
    return click.argument(
        "component", nargs=1, required=False, callback=_check_component
    )(func)
