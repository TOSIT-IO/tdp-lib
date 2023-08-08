# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from tdp.core.models.stale_component import StaleComponent


def test_stale_components_to_dict():
    stale_component1 = StaleComponent(
        service_name="service",
        component_name="component1",
        to_reconfigure=True,
        to_restart=True,
    )
    stale_component2 = StaleComponent(
        service_name="service",
        component_name="component2",
        to_reconfigure=False,
        to_restart=True,
    )
    stale_components_dict = StaleComponent.to_dict([stale_component1, stale_component2])

    assert stale_components_dict == {
        ("service", "component1"): stale_component1,
        ("service", "component2"): stale_component2,
    }


def test_stale_components_to_dict_empty():
    stale_components_dict = StaleComponent.to_dict([])
    assert stale_components_dict == {}


def test_stale_components_to_dict_duplicate():
    stale_component1 = StaleComponent(
        service_name="service",
        component_name="component",
        to_reconfigure=True,
        to_restart=True,
    )
    stale_component2 = StaleComponent(
        service_name="service",
        component_name="component",
        to_reconfigure=False,
        to_restart=True,
    )
    stale_components_dict = StaleComponent.to_dict([stale_component1, stale_component2])

    assert stale_components_dict == {
        ("service", "component"): stale_component2,
    }


def test_stale_components_to_dict_empty_component():
    stale_component1 = StaleComponent(
        service_name="service",
        component_name="",
        to_reconfigure=True,
        to_restart=True,
    )
    stale_component2 = StaleComponent(
        service_name="service",
        component_name="component",
        to_reconfigure=False,
        to_restart=True,
    )
    stale_components_dict = StaleComponent.to_dict([stale_component1, stale_component2])

    assert stale_components_dict == {
        ("service", ""): stale_component1,
        ("service", "component"): stale_component2,
    }
