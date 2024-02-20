# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from pathlib import Path

from tdp.core.constants import HOST_NAME_MAX_LENGTH
from tdp.core.entities.operation_name import OperationName

# TODO: create constructors


@dataclass(frozen=True)
class Operation(ABC):
    name: OperationName


@dataclass(frozen=True)
class PlaybookOperation(Operation, ABC):
    playbook: Playbook


@dataclass(frozen=True)
class DagOperation(Operation, ABC):
    depends_on: set[str]


@dataclass(frozen=True)
class DagOperationNoop(DagOperation):
    pass


@dataclass(frozen=True)
class DagOperationWithPlaybook(DagOperation, PlaybookOperation):
    pass


@dataclass(frozen=True)
class OtherOperation(PlaybookOperation):
    pass


@dataclass(frozen=True)
class Playbook:
    playbook_path: Path
    collection_name: str
    host_names: frozenset[str]

    def __post_init__(self):
        for host_name in self.host_names:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise ValueError(
                    f"Host '{host_name}' must be less than {HOST_NAME_MAX_LENGTH} "
                    "characters."
                )
