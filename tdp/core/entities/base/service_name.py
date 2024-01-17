# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

from tdp.core.constants import SERVICE_NAME_MAX_LENGTH
from tdp.core.entities import NamedEntity


@dataclass(frozen=True)
class ServiceName(NamedEntity):
    name: str

    def __post_init__(self):
        if len(self.name) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(
                f"Service '{self.name}' must be less than {SERVICE_NAME_MAX_LENGTH} "
                "characters."
            )
