# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from typing import Optional

from tdp.core.entities.hosted_entity import HostedEntity


@dataclass(frozen=True)
class HostedEntityStatus:
    entity: HostedEntity
    running_version: Optional[str] = None
    configured_version: Optional[str] = None
    to_config: Optional[bool] = None
    to_restart: Optional[bool] = None
    is_active: Optional[bool] = None
