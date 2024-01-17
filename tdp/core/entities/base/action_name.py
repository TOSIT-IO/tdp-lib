# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass

from tdp.core.constants import ACTION_NAME_MAX_LENGTH
from tdp.core.entities import NamedEntity


@dataclass(frozen=True)
class ActionName(NamedEntity):
    name: str

    def __post_init__(self):
        if len(self.name) > ACTION_NAME_MAX_LENGTH:
            raise ValueError(
                f"Action '{self.name}' must be less than {ACTION_NAME_MAX_LENGTH} "
                "characters."
            )
