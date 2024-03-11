# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from pathlib import Path

from tdp.core.constants import HOST_NAME_MAX_LENGTH


@dataclass(frozen=True)
class Playbook:
    path: Path
    collection_name: str
    hosts: frozenset[str]

    def __post_init__(self):
        for host_name in self.hosts:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise ValueError(
                    f"Host '{host_name}' must be less than {HOST_NAME_MAX_LENGTH} "
                    "characters."
                )
