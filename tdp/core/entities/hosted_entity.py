# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from dataclasses import dataclass

from tdp.core.entities import DataclassEnforcer, NamedEntity
from tdp.core.entities.base.service_component_name import ServiceComponentName
from tdp.core.entities.base.service_name import ServiceName


class HostedEntity(NamedEntity, ABC, metaclass=DataclassEnforcer):
    @property
    @abstractmethod
    def host(self) -> str:
        pass


@dataclass(frozen=True)
class HostedService(ServiceName, HostedEntity):
    host: str


@dataclass(frozen=True)
class HostedServiceComponent(ServiceComponentName, HostedEntity):
    host: str
