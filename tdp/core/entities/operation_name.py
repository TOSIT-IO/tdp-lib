# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from tdp.core.constants import ACTION_NAME_MAX_LENGTH
from tdp.core.entities.hostable_entity_name import (
    ServiceComponentName,
    ServiceName,
    create_hostable_entity_name,
)


@dataclass(frozen=True)
class ActionName:
    name: str

    def __post_init__(self):
        if len(self.name) > ACTION_NAME_MAX_LENGTH:
            raise ValueError(
                f"Action '{self.name}' must be less than {ACTION_NAME_MAX_LENGTH} "
                "characters."
            )
        if not self.name:
            raise ValueError("Action name cannot be empty.")

    def __str__(self):
        return self.name


class OperationName(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    @abstractmethod
    def action(self) -> ActionName:
        pass

    @property
    @abstractmethod
    def service(self) -> ServiceName:
        pass


@dataclass(frozen=True)
class ServiceOperationName(OperationName):
    _service: ServiceName
    _action: ActionName

    @property
    def action(self) -> ActionName:
        return self._action

    @property
    def name(self) -> str:
        return f"{self.service.name}_{self._action.name}"

    @property
    def service(self) -> ServiceName:
        return self._service

    def __str__(self):
        return self.name


@dataclass(frozen=True)
class ServiceComponentOperationName(OperationName):
    service_component: ServiceComponentName
    _action: ActionName

    @property
    def action(self) -> ActionName:
        return self._action

    @property
    def name(self) -> str:
        return f"{self.service_component.name}_{self._action.name}"

    @property
    def service(self) -> ServiceName:
        return self.service_component.service

    def __str__(self):
        return self.name


def create_operation_name(name: str) -> OperationName:
    entity_name, action_name = name.rsplit("_", 1)
    entity = create_hostable_entity_name(entity_name)
    if isinstance(entity, ServiceName):
        return ServiceOperationName(entity, ActionName(action_name))
    elif isinstance(entity, ServiceComponentName):
        return ServiceComponentOperationName(entity, ActionName(action_name))
    else:
        raise ValueError(f"Invalid operation name '{name}'.")
