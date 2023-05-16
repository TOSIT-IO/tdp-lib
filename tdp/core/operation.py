# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import re

# service operation: <service>_<action>
RE_IS_SERVICE = re.compile("^([^_]+)_[^_]+$")
# component operation: <service>_<component>_<action>
RE_GET_SERVICE = re.compile("^([^_]+)_.*")
RE_GET_COMPONENT = re.compile("^[^_]+_(.*)_[^_]+$")
RE_GET_ACTION = re.compile(".*_([^_]+)$")


SERVICE_NAME_MAX_LENGTH = 20
COMPONENT_NAME_MAX_LENGTH = 30
ACTION_NAME_MAX_LENGTH = 20

OPERATION_NAME_MAX_LENGTH = (
    SERVICE_NAME_MAX_LENGTH + COMPONENT_NAME_MAX_LENGTH + ACTION_NAME_MAX_LENGTH
)


class Operation:
    def __init__(self, name, collection_name=None, depends_on=None, noop=False):
        self.name = name
        self.collection_name = collection_name
        self.depends_on = depends_on or []
        self.noop = noop

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
                f"action {self.service_name} is longer than {ACTION_NAME_MAX_LENGTH}"
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

    def is_service_operation(self) -> bool:
        """Return True if the operation is about a service, False otherwise"""
        return bool(RE_IS_SERVICE.search(self.name))

    def __repr__(self):
        return (
            f"Operation(name={self.name}, "
            f"collection_name={self.collection_name}, "
            f"depends_on={self.depends_on}, "
            f"noop={self.noop})"
        )
