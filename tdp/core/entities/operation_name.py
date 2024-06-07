# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Union

from tdp.core.constants import ACTION_NAME_MAX_LENGTH
from tdp.core.entities.hostable_entity_name import (
    HostableEntityName,
    ServiceComponentName,
    ServiceName,
    parse_hostable_entity_name,
)


@dataclass(frozen=True)
class OperationName(ABC):
    entity: HostableEntityName
    action: str

    @property
    def name(self) -> str:
        return f"{self.entity.name}_{self.action}"

    def __post_init__(self):
        if len(self.name) > ACTION_NAME_MAX_LENGTH:
            raise ValueError(
                f"Action '{self.name}' must be less than {ACTION_NAME_MAX_LENGTH} "
                "characters."
            )
        if not self.name:
            raise ValueError("Action name cannot be empty.")

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class ServiceOperationName(OperationName):
    entity: ServiceName


@dataclass(frozen=True)
class ServiceComponentOperationName(OperationName):
    entity: ServiceComponentName


def parse_operation_name(
    name: str,
) -> Union[ServiceOperationName, ServiceComponentOperationName]:
    entity_name, action_name = name.rsplit("_", 1)
    entity = parse_hostable_entity_name(entity_name)
    if isinstance(entity, ServiceName):
        return ServiceOperationName(entity, action_name)
    elif isinstance(entity, ServiceComponentName):
        return ServiceComponentOperationName(entity, action_name)
    else:
        raise ValueError(f"Invalid operation name '{name}'.")
