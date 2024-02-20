# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, Optional

from tdp.core.service_component_name import OldServiceComponentName


class OldServiceComponentHostName:
    """Represent a service or a component name with an optional host."""

    def __init__(
        self,
        service_component_name: OldServiceComponentName,
        host_name: Optional[str] = None,
    ):
        self.service_component_name = service_component_name
        self.host_name = host_name

    @property
    def full_name(self) -> str:
        """Full name of the service or component."""
        return self.service_component_name.full_name

    @property
    def is_service(self) -> bool:
        """True if the component is a service."""
        return self.service_component_name.is_service

    @staticmethod
    def from_full_host_name(
        full_name: str, host_name: Optional[str] = None
    ) -> OldServiceComponentHostName:
        """Factory method to build ServiceComponentHostName from a full name and an optional host.

        Args:
            full_name: Full name of the service or component.
            host_name: Host name of the service or component.

        Returns:
            ServiceComponentHostName instance.
        """
        return OldServiceComponentHostName(
            service_component_name=OldServiceComponentName.from_full_name(full_name),
            host_name=host_name,
        )

    def __repr__(self) -> str:
        return (
            "ServiceComponentHostName("
            f"{self.service_component_name.service_name}, "
            f"{self.service_component_name.component_name}, "
            f"{self.host_name})"
        )

    def __str__(self) -> str:
        return self.full_name

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, OldServiceComponentHostName):
            return NotImplemented
        return repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash(repr(self))
