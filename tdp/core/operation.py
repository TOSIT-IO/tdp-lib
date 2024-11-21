# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Any, Optional, Union

from attr import dataclass

from tdp.core.constants import (
    ACTION_NAME_MAX_LENGTH,
    HOST_NAME_MAX_LENGTH,
    OPERATION_NAME_MAX_LENGTH,
)
from tdp.core.entities.entity_name import (
    ServiceComponentName,
    ServiceName,
    parse_entity_name,
)


@dataclass(frozen=True)
class OperationName:
    entity: Union[ServiceName, ServiceComponentName]
    action: str

    def __post_init__(self):
        if len(self.action) > ACTION_NAME_MAX_LENGTH:
            raise ValueError(
                f"Action '{self.action}' must be less than {ACTION_NAME_MAX_LENGTH} "
                "characters."
            )
        if not self.action:
            raise ValueError("Action name cannot be empty.")
        if len(self.name) > OPERATION_NAME_MAX_LENGTH:
            raise ValueError(
                f"Operation '{self.name}' must be less than {OPERATION_NAME_MAX_LENGTH}"
                " characters."
            )

    @property
    def service(self) -> str:
        return self.entity.service

    @property
    def component(self) -> Optional[str]:
        return getattr(self.entity, "component", None)

    @property
    def name(self) -> str:
        return f"{self.entity.name}_{self.action}"

    @classmethod
    def from_name(cls, name: str) -> OperationName:
        entity_name, action_name = name.rsplit("_", 1)
        entity = parse_entity_name(entity_name)
        return cls(entity, action_name)

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class Operation:
    """A task that can be executed by Ansible.

    The name of the operation is composed of the service name, the component name and
    the action name (<service>_<component>_<action>). The component name is optional.

    Args:
        action: Name of the action.
        name: Name of the operation.
        collection_name: Name of the collection where the operation is defined.
        component: Name of the component.
        depends_on: List of operations that must be executed before this one.
        noop: If True, the operation will not be executed.
        service: Name of the service.
        host_names: Set of host names where the operation can be launched.
    """

    def __init__(
        self,
        name: str,
        collection_name: Optional[str] = None,
        depends_on: Optional[list[str]] = None,
        noop: bool = False,
        host_names: Optional[set[str]] = None,
    ):
        """Create a new Operation.

        Args:
            name: Name of the operation.
            collection_name: Name of the collection where the operation is defined.
            depends_on: List of operations that must be executed before this one.
            noop: If True, the operation will not be executed.
            host_names: Set of host names where the operation can be launched.
        """
        self.name = OperationName.from_name(name)
        self.str_name = self.name.name
        self.collection_name = collection_name
        self.depends_on = depends_on or []
        self.noop = noop
        self.host_names = host_names or set()

        for host_name in self.host_names:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise ValueError(
                    f"host {host_name} is longer than {HOST_NAME_MAX_LENGTH}"
                )

    def is_service_operation(self) -> bool:
        """Return True if the operation is about a service, False otherwise."""
        return isinstance(self.name.entity, ServiceName)

    def __repr__(self):
        return (
            f"Operation(name={self.str_name}, "
            f"collection_name={self.collection_name}, "
            f"depends_on={self.depends_on}, "
            f"noop={self.noop}, "
            f"host_names={self.host_names})"
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Operation):
            return NotImplemented
        return repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash(repr(self))
