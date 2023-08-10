# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


from tdp.core.models.stale_component import StaleComponent


def test_stale_components_to_dict():
    stale_component1 = StaleComponent(
        service_name="service",
        component_name="component1",
        host_name="host1",
        to_reconfigure=True,
        to_restart=True,
    )
    stale_component2 = StaleComponent(
        service_name="service",
        component_name="component2",
        host_name="host2",
        to_reconfigure=False,
        to_restart=True,
    )
    stale_components_dict = StaleComponent.to_dict([stale_component1, stale_component2])

    assert stale_components_dict == {
        ("service", "component1", "host1"): stale_component1,
        ("service", "component2", "host2"): stale_component2,
    }


def test_stale_components_to_dict_empty():
    stale_components_dict = StaleComponent.to_dict([])
    assert stale_components_dict == {}


def test_stale_components_to_dict_duplicate():
    stale_component1 = StaleComponent(
        service_name="service",
        component_name="component",
        host_name="host",
        to_reconfigure=True,
        to_restart=True,
    )
    stale_component2 = StaleComponent(
        service_name="service",
        component_name="component",
        host_name="host",
        to_reconfigure=False,
        to_restart=True,
    )
    stale_components_dict = StaleComponent.to_dict([stale_component1, stale_component2])

    assert stale_components_dict == {
        ("service", "component", "host"): stale_component2,
    }


def test_stale_components_to_dict_empty_component():
    stale_component1 = StaleComponent(
        service_name="service",
        component_name="",
        host_name="host",
        to_reconfigure=True,
        to_restart=True,
    )
    stale_component2 = StaleComponent(
        service_name="service",
        component_name="component",
        host_name="host",
        to_reconfigure=False,
        to_restart=True,
    )
    stale_components_dict = StaleComponent.to_dict([stale_component1, stale_component2])

    assert stale_components_dict == {
        ("service", "", "host"): stale_component1,
        ("service", "component", "host"): stale_component2,
    }
