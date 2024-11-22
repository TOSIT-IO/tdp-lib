# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC
from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Union

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
            f"Operation(name={self.name}, "
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

    @property
    def name(self) -> str:
        return self.path.stem


@dataclass(frozen=True)
class BaseOperation(ABC):

    name: OperationName

    @classmethod
    def from_name(
        cls,
        name: str,
    ) -> BaseOperation:
        return cls(OperationName.from_name(name))


@dataclass(frozen=True)
class DagOperation(BaseOperation):
    """A DAG node.

    Args:
        name: Name of the operation.
        depends_on: List of operations that must be executed before this one.
        definitions: Set of paths to the playbooks that define the node.
    """

    depends_on: frozenset[str]

    @classmethod
    def from_name(
        cls,
        name: str,
        depends_on: Optional[frozenset[str]] = None,
    ) -> DagOperation:
        return cls(
            name=OperationName.from_name(name),
            depends_on=depends_on or frozenset(),
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
