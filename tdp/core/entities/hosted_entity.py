# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from dataclasses import dataclass

from tdp.core.entities.hostable_entity_name import (
    ComponentName,
    HostableEntityName,
    ServiceComponentName,
    ServiceName,
)


class HostedEntity(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def host(self) -> str:
        pass

    @property
    @abstractmethod
    def service(self) -> ServiceName:
        pass


@dataclass(frozen=True)
class HostedService(HostedEntity):
    _service: ServiceName
    _host: str

    @property
    def name(self) -> str:
        return self._service.name

    @property
    def host(self) -> str:
        return self._host

    @property
    def service(self) -> ServiceName:
        return self._service


@dataclass(frozen=True)
class HostedServiceComponent(HostedEntity):
    service_component: ServiceComponentName
    _host: str

    @property
    def service(self) -> ServiceName:
        return self.service_component.service

    @property
    def name(self) -> str:
        return self.service_component.name

    @property
    def host(self) -> str:
        return self._host

    @property
    def component(self) -> ComponentName:
        return self.service_component.component


def create_hosted_entity(name: HostableEntityName, host: str) -> HostedEntity:
    if isinstance(name, ServiceName):
        return HostedService(name, host)
    elif isinstance(name, ServiceComponentName):
        return HostedServiceComponent(name, host)
    else:
        raise ValueError(f"Unknown name type: {name}")
