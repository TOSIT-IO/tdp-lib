# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Union

from tdp.core.constants import COMPONENT_NAME_MAX_LENGTH, SERVICE_NAME_MAX_LENGTH


@dataclass(frozen=True)
class EntityName(ABC):
    service: str

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    def __post_init__(self):
        if len(self.service) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(
                f"Service '{self.service}' must be less than {SERVICE_NAME_MAX_LENGTH} "
                "characters."
            )
        if not self.service:
            raise ValueError("Service name cannot be empty.")

    def __str__(self):
        return self.name


@dataclass(frozen=True)
class ServiceName(EntityName):

    @property
    def name(self) -> str:
        return self.service


@dataclass(frozen=True)
class ServiceComponentName(EntityName):
    component: str

    def __post_init__(self):
        super().__post_init__()
        if len(self.component) > COMPONENT_NAME_MAX_LENGTH:
            raise ValueError(
                f"Component '{self.component}' must be less than "
                "{COMPONENT_NAME_MAX_LENGTH} characters."
            )
        if not self.component:
            raise ValueError("Component name cannot be empty.")

    @property
    def name(self) -> str:
        return f"{self.service}_{self.component}"

    @classmethod
    def from_name(cls, name: str) -> ServiceComponentName:
        service_name, component_name = name.split("_", 1)
        return cls(service_name, component_name)


def parse_entity_name(name: str) -> Union[ServiceName, ServiceComponentName]:
    if "_" in name:
        return ServiceComponentName.from_name(name)
    return ServiceName(name)


def create_entity_name(
    service_name: str, component_name: Optional[str]
) -> Union[ServiceName, ServiceComponentName]:
    if component_name is None:
        return ServiceName(service_name)
    return ServiceComponentName(service_name, component_name)
