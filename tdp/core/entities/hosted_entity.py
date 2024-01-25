# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC
from dataclasses import dataclass

from tdp.core.entities.hostable_entity_name import (
    ComponentName,
    HostableEntityName,
    ServiceComponentName,
    ServiceName,
)


@dataclass(frozen=True)
class HostedEntity(HostableEntityName, ABC):
    host: str


@dataclass(frozen=True)
class HostedService(HostedEntity, ServiceName):
    pass


@dataclass(frozen=True)
class HostedServiceComponent(HostedEntity, ServiceComponentName):
    @classmethod
    def from_name(cls, name: str, host: str) -> ServiceComponentName:
        service_name, component_name = name.split("_", 1)
        return cls(ServiceName(service_name), ComponentName(component_name), host)
