# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Union

from tdp.core.entities.hostable_entity_name import (
    HostableEntityName,
    ServiceComponentName,
    ServiceName,
)


@dataclass(frozen=True)
class HostedEntity(ABC):
    @property
    @abstractmethod
    def name(self) -> HostableEntityName:
        pass

    @property
    @abstractmethod
    def host(self) -> Optional[str]:
        pass


@dataclass(frozen=True)
class HostedService(HostedEntity):
    _service: ServiceName
    _host: Optional[str]

    @property
    def name(self) -> ServiceName:
        return self._service

    @property
    def host(self) -> Optional[str]:
        return self._host


@dataclass(frozen=True)
class HostedServiceComponent(HostedEntity):
    service_component: ServiceComponentName
    _host: Optional[str]

    @property
    def name(self) -> ServiceComponentName:
        return self.service_component

    @property
    def host(self) -> Optional[str]:
        return self._host


def create_hosted_entity(
    name: HostableEntityName, host: Optional[str]
) -> Union[HostedService, HostedServiceComponent]:
    if isinstance(name, ServiceName):
        return HostedService(name, host)
    elif isinstance(name, ServiceComponentName):
        return HostedServiceComponent(name, host)
    else:
        raise ValueError(f"Unknown name type: {name}")
