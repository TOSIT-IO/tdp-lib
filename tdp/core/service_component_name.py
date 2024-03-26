# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, Optional


class ServiceComponentName:
    """Represent a service or a component name."""

    def __init__(self, service_name: str, component_name: Optional[str] = None):
        self.service_name = service_name
        self.component_name = component_name

    @property
    def full_name(self) -> str:
        """Full name of the service or component."""
        return (
            self.service_name
            if self.component_name is None
            else f"{self.service_name}_{self.component_name}"
        )

    @property
    def is_service(self) -> bool:
        """True if the component is a service."""
        return self.component_name is None

    @staticmethod
    def from_full_name(full_name: str) -> ServiceComponentName:
        """Factory method to build ServiceComponentName from a full name.

        Args:
            full_name: Full name of the service or component.

        Returns:
            ServiceComponentName instance.
        """
        [service_name, *component_name] = full_name.split("_", 1)
        component_name = component_name[0] if component_name else None
        return ServiceComponentName(service_name, component_name)

    def __repr__(self) -> str:
        return f"ServiceComponentName({self.service_name}, {self.component_name})"

    def __str__(self) -> str:
        return self.full_name

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ServiceComponentName):
            return NotImplemented
        return repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash(repr(self))
