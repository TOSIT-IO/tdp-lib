# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.core.constants import SERVICE_NAME_MAX_LENGTH
from tdp.core.entities.hostable_entity_name import (
    ComponentName,
    ServiceComponentName,
    ServiceName,
    parse_hostable_entity_name,
)


def test_service_name():
    service_name = ServiceName("s")
    assert str(service_name) == "s"


def test_service_name_max_length():
    long_name = "s" * (SERVICE_NAME_MAX_LENGTH + 1)
    with pytest.raises(ValueError):
        ServiceName(long_name)


def test_service_component_name():
    service_component_name = ServiceComponentName(ServiceName("s"), ComponentName("c"))
    assert str(service_component_name.service) == "s"
    assert str(service_component_name.component) == "c"
    assert str(service_component_name) == "s_c"


def test_service_component_name_from_name():
    service_component_name = ServiceComponentName.from_name("s_c")
    assert str(service_component_name) == "s_c"
    assert str(service_component_name.service) == "s"
    assert str(service_component_name.component) == "c"


def test_service_component_name_from_long_name():
    service_component_name = ServiceComponentName.from_name("s_c_c")
    assert str(service_component_name) == "s_c_c"
    assert str(service_component_name.service) == "s"
    assert str(service_component_name.component) == "c_c"


def test_service_component_name_from_name_invalid():
    with pytest.raises(ValueError):
        ServiceComponentName.from_name("s")
    with pytest.raises(ValueError):
        ServiceComponentName.from_name("s_")


def test_create_hostable_entity_name_service_name():
    hostable_entity_name = parse_hostable_entity_name("s")
    assert isinstance(hostable_entity_name, ServiceName)
    assert hostable_entity_name.name == "s"


def test_create_hostable_entity_name_service_component_name():
    hostable_entity_name = parse_hostable_entity_name("s_c")
    assert isinstance(hostable_entity_name, ServiceComponentName)
    assert str(hostable_entity_name.service) == "s"
    assert str(hostable_entity_name.component) == "c"
    assert str(hostable_entity_name) == "s_c"
