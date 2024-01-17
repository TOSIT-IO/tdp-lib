# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

from tdp.core.constants import COMPONENT_NAME_MAX_LENGTH
from tdp.core.entities import NamedEntity


@dataclass(frozen=True)
class ComponentName(NamedEntity):
    name: str

    def __post_init__(self):
        if len(self.name) > COMPONENT_NAME_MAX_LENGTH:
            raise ValueError(
                f"Component '{self.name}' must be less than "
                "{COMPONENT_NAME_MAX_LENGTH} characters."
            )
