# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.core.entities.hostable_entity_name import ServiceComponentName, ServiceName
from tdp.core.entities.hosted_entity import create_hosted_entity
from tdp.core.entities.hosted_entity_status import HostedEntityStatus


def test_service_entity_status():
    entity = create_hosted_entity(ServiceName("s"), "h")
    running_version = "1.0.0"
    configured_version = "1.1.0"
    to_config = True
    to_restart = False

    status = HostedEntityStatus(
        entity, running_version, configured_version, to_config, to_restart
    )

    assert isinstance(status, HostedEntityStatus)
    assert status.entity == entity
    assert status.running_version == running_version
    assert status.configured_version == configured_version
    assert status.to_config == to_config
    assert status.to_restart == to_restart


def test_service_component_entity_status():
    entity = create_hosted_entity(ServiceComponentName.from_name("s_c"), "h")
    running_version = "1.0.0"
    configured_version = "1.1.0"
    to_config = True
    to_restart = False

    status = HostedEntityStatus(
        entity, running_version, configured_version, to_config, to_restart
    )

    assert isinstance(status, HostedEntityStatus)
    assert status.entity == entity
    assert status.running_version == running_version
    assert status.configured_version == configured_version
    assert status.to_config == to_config
    assert status.to_restart == to_restart
