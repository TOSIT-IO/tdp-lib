# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from tdp.core.constants import HOST_NAME_MAX_LENGTH
from tdp.core.entities.operation_name import OperationName

# TODO: create constructors


@dataclass(frozen=True)
class Operation(ABC):
    name: OperationName


@dataclass(frozen=True)
class DagOperation(Operation):
    depends_on: set[str]
    playbook: Optional[PlaybookOperation]


@dataclass(frozen=True)
class OtherOperation(Operation):
    playbook: PlaybookOperation
    pass


@dataclass(frozen=True)
class PlaybookOperation:
    playbook_path: Path
    collection_name: str
    host_names: set[str]

    def __post_init__(self):
        for host_name in self.host_names:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise ValueError(
                    f"Host '{host_name}' must be less than {HOST_NAME_MAX_LENGTH} "
                    "characters."
                )
