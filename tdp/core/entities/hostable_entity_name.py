# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from tdp.core.constants import COMPONENT_NAME_MAX_LENGTH, SERVICE_NAME_MAX_LENGTH


class HostableEntityName(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass


@dataclass(frozen=True)
class ServiceName(HostableEntityName):
    _name: str

    @property
    def name(self) -> str:
        return self._name

    def __post_init__(self):
        if len(self._name) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(
                f"Service '{self._name}' must be less than {SERVICE_NAME_MAX_LENGTH} "
                "characters."
            )
        if not self._name:
            raise ValueError("Service name cannot be empty.")

    def __str__(self):
        return self._name


@dataclass(frozen=True)
class ComponentName:
    name: str

    def __post_init__(self):
        if len(self.name) > COMPONENT_NAME_MAX_LENGTH:
            raise ValueError(
                f"Component '{self.name}' must be less than "
                "{COMPONENT_NAME_MAX_LENGTH} characters."
            )
        if not self.name:
            raise ValueError("Component name cannot be empty.")

    def __str__(self):
        return self.name


@dataclass(frozen=True)
class ServiceComponentName(HostableEntityName):
    service: ServiceName
    component: ComponentName

    @property
    def name(self) -> str:
        return f"{self.service.name}_{self.component.name}"

    @classmethod
    def from_name(cls, name: str) -> ServiceComponentName:
        service_name, component_name = name.split("_", 1)
        return cls(ServiceName(service_name), ComponentName(component_name))

    def __str__(self):
        return self.name


def parse_hostable_entity_name(name: str) -> HostableEntityName:
    if "_" in name:
        return ServiceComponentName.from_name(name)
    return ServiceName(name)
