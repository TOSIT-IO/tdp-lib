# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path

from tdp.core.constants import HOST_NAME_MAX_LENGTH
from tdp.core.operation import Operation


@dataclass(frozen=True)
class Playbook:
    path: Path
    collection_name: str
    hosts: set[str]  # TODO: would be better to use a frozenset

    def __post_init__(self):
        for host_name in self.hosts:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise ValueError(
                    f"Host '{host_name}' must be less than {HOST_NAME_MAX_LENGTH} "
                    "characters."
                )


class Operations(MutableMapping[str, Operation]):

    def __init__(self):
        self._operations: dict[str, Operation] = {}

    def __getitem__(self, key: str):
        try:
            return self._operations[key]
        except KeyError:
            raise KeyError(f"Operation '{key}' not found")

    def __setitem__(self, key: str, value: Operation):
        if key != value.name.name:
            raise ValueError(
                f"Operation name '{value.name}' does not match key '{key}'"
            )
        self._operations[key] = value

    def __delitem__(self, key):
        del self._operations[key]

    def __iter__(self):
        return iter(self._operations)

    def __len__(self):
        return len(self._operations)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._operations})"

    def __str__(self):
        return f"{self.__class__.__name__}({self._operations})"

    def get(self, key: str, default=None, *, restart: bool = False, stop: bool = False):
        if restart and key.endswith("_start"):
            key = key.replace("_start", "_restart")
        elif stop and key.endswith("_start"):
            key = key.replace("_start", "_stop")
        return self._operations.get(key, default)
