# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

"""
This module contains base tests that will be used to generate tests for each rule in the
rules directory of each collection.
"""


from collections.abc import Iterable

from tdp.core.collections import Collections
from test_dag_order.helpers import resolve_components


def _add_source_service(
    children: Iterable[str], service_component_map: dict[str, str]
) -> list[str]:
    """Add the source service to the children if it is in the service_component_map."""
    formatted_children = []
    for child in children:
        if child in service_component_map:
            formatted_children.append(f"{child} ({service_component_map[child]})")
        else:
            formatted_children.append(child)
    return formatted_children


class MissingChildrenError(Exception):
    """Raised when a DAG node is missing children"""

    def __init__(
        self,
        short_msg: str,
        missing: Iterable[str],
        current: Iterable[str],
    ):
        self.short_msg = short_msg
        self.missing = missing
        self.current = current

    def __str__(self):
        current_children = "\n\t\t".join(sorted(self.current))
        missing_children = "\n\t\t".join(sorted(self.missing))
        return (
            f"{self.short_msg}\n"
            f"\tMissing children:\n"
            f"\t\t{missing_children}\n"
            f"\tCurrent children:\n"
            f"\t\t{current_children}"
        )


class ExtraChildrenError(Exception):
    """Raised when a DAG node has extra children"""

    def __init__(
        self,
        short_msg: str,
        extra: Iterable[str],
        current: Iterable[str],
    ):
        self.short_msg = short_msg
        self.extra = extra
        self.current = current

    def __str__(self):
        current_children = "\n\t\t".join(sorted(self.current))
        extra_children = "\n\t\t".join(sorted(self.extra))
        return (
            f"{self.short_msg}\n"
            f"\tExtra children:\n"
            f"\t\t{extra_children}\n"
            f"\tCurrent children:\n"
            f"\t\t{current_children}"
        )


def test_must_include_and_must_exclude_should_not_intersect(
    must_include: set[str], must_exclude: set[str]
):
    intersection = must_include.intersection(must_exclude)
    assert (
        intersection == set()
    ), f"must_include and must_exclude should not intersect: {', '.join(intersection)}"


def test_reconfigure_plan_has_included_services(
    source: str, must_include: set[str], stale_sc: list[str], collections: Collections
):
    # must_include can include services, in which case we need to resolve the components
    [resolved_components, service_component_map] = resolve_components(
        must_include, collections
    )
    difference = resolved_components.difference(stale_sc)
    if len(difference) > 0:
        raise MissingChildrenError(
            f"{source} reconfiguration should include: {', '.join(difference)}",
            _add_source_service(difference, service_component_map),
            stale_sc,
        )


def test_reconfigure_plan_does_not_have_excluded_services(
    source: str, must_exclude: set, stale_sc: set, collections: Collections
):
    # must_exclude can include services, in which case we need to resolve the components
    [resolved_components, service_component_map] = resolve_components(
        must_exclude, collections
    )
    intersection = resolved_components.intersection(stale_sc)
    if len(intersection) > 0:
        raise ExtraChildrenError(
            f"{source} reconfiguration should not include: {', '.join(intersection)}",
            _add_source_service(intersection, service_component_map),
            stale_sc,
        )
