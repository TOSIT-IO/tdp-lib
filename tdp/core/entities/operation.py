# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from tdp.core.constants import HOST_NAME_MAX_LENGTH
from tdp.core.entities.operation_name import OperationName


@dataclass(frozen=True)
class Operation(ABC):
    name: OperationName
    collection_name: str
    depends_on: set[str]


@dataclass(frozen=True)
class PlaybookOperation(Operation):
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
    pass
