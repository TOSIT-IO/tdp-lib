# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

from tdp.core.entities import NamedEntity
from tdp.core.entities.base.component_name import ComponentName
from tdp.core.entities.base.service_component_name import ServiceComponentName
from tdp.core.entities.base.service_name import ServiceName


@dataclass(frozen=True)
class ServiceComponentName(NamedEntity):
    service: ServiceName
    component: ComponentName

    @property
    def name(self) -> str:
        return f"{self.service.name}_{self.component.name}"

    @classmethod
    def from_name(cls, name: str) -> ServiceComponentName:
        service_name, component_name = name.split("_")
        return cls(ServiceName(service_name), ComponentName(component_name))
