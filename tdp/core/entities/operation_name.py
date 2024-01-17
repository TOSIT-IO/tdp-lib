# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from dataclasses import dataclass

from tdp.core.entities import DataclassEnforcer, NamedEntity
from tdp.core.entities.base.action_name import ActionName
from tdp.core.entities.base.service_component_name import ServiceComponentName
from tdp.core.entities.base.service_name import ServiceName


class OperationName(NamedEntity, ABC, metaclass=DataclassEnforcer):
    @property
    @abstractmethod
    def action(self) -> ActionName:
        pass


@dataclass(frozen=True)
class ServiceOperationName(OperationName):
    service: ServiceName
    action: ActionName

    @property
    def name(self) -> str:
        return f"{self.service.name}_{self.action.name}"


@dataclass(frozen=True)
class ServiceComponentOperationName(OperationName):
    service_component: ServiceComponentName
    action: ActionName

    @property
    def name(self) -> str:
        return f"{self.service_component.name}_{self.action.name}"
