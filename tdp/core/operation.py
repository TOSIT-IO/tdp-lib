# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import re
from typing import Any, Optional

from tdp.core.constants import (
    ACTION_NAME_MAX_LENGTH,
    COMPONENT_NAME_MAX_LENGTH,
    HOST_NAME_MAX_LENGTH,
    OPERATION_NAME_MAX_LENGTH,
    SERVICE_NAME_MAX_LENGTH,
)

# service operation: <service>_<action>
RE_IS_SERVICE = re.compile("^([^_]+)_[^_]+$")
# component operation: <service>_<component>_<action>
RE_GET_SERVICE = re.compile("^([^_]+)_.*")
RE_GET_COMPONENT = re.compile("^[^_]+_(.*)_[^_]+$")
RE_GET_ACTION = re.compile(".*_([^_]+)$")


class LegacyOperation:
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
        self.name = name
        self.collection_name = collection_name
        self.depends_on = depends_on or []
        self.noop = noop
        self.host_names = host_names or set()

        if len(name) > OPERATION_NAME_MAX_LENGTH:
            raise ValueError(f"{name} is longer than {OPERATION_NAME_MAX_LENGTH}")

        match = RE_GET_SERVICE.search(self.name)
        if not match:
            raise ValueError(f"Fail to parse service name from '{self.name}'")
        self.service_name = match.group(1)

        if len(self.service_name) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(
                f"service {self.service_name} is longer than {SERVICE_NAME_MAX_LENGTH}"
            )

        match = RE_GET_ACTION.search(self.name)
        if not match:
            raise ValueError(f"Fail to parse action name from '{self.name}'")
        self.action_name = match.group(1)

        if len(self.action_name) > ACTION_NAME_MAX_LENGTH:
            raise ValueError(
                f"action {self.action_name} is longer than {ACTION_NAME_MAX_LENGTH}"
            )

        match = RE_GET_COMPONENT.search(self.name)
        if not match:
            self.component_name = None
        else:
            self.component_name = match.group(1)
        if (
            self.component_name is not None
            and len(self.component_name) > COMPONENT_NAME_MAX_LENGTH
        ):
            raise ValueError(
                f"component {self.component_name} is longer than {COMPONENT_NAME_MAX_LENGTH}"
            )

        for host_name in self.host_names:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise ValueError(
                    f"host {host_name} is longer than {HOST_NAME_MAX_LENGTH}"
                )

    def is_service_operation(self) -> bool:
        """Return True if the operation is about a service, False otherwise."""
        return bool(RE_IS_SERVICE.search(self.name))

    def __repr__(self):
        return (
            f"Operation(name={self.name}, "
            f"collection_name={self.collection_name}, "
            f"depends_on={self.depends_on}, "
            f"noop={self.noop}, "
            f"host_names={self.host_names})"
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LegacyOperation):
            return NotImplemented
        return repr(self) == repr(other)

    def __hash__(self) -> int:
        return hash(repr(self))
