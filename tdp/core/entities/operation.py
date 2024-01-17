# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from dataclasses import dataclass

from tdp.core.constants import HOST_NAME_MAX_LENGTH
from tdp.core.entities import DataclassEnforcer, NamedEntity
from tdp.core.entities.operation_name import OperationName


class Operation(NamedEntity, ABC, metaclass=DataclassEnforcer):
    @property
    @abstractmethod
    def collection_name(self) -> str:
        pass

    @property
    @abstractmethod
    def depends_on(self) -> set[str]:
        pass


@dataclass(frozen=True)
class PlaybookOperation(Operation):
    name: OperationName
    collection_name: str
    depends_on: set[str]
    host_names: set[str]

    def __post_init__(self):
        for host_name in self.host_names:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise ValueError(
                    f"Host '{host_name}' must be less than {HOST_NAME_MAX_LENGTH} "
                    "characters."
                )


@dataclass(frozen=True)
class NoOperation(Operation):
    name: str
    collection_name: str
    depends_on: set[str]
