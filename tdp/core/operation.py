# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import re

logger = logging.getLogger("tdp").getChild("component")

# service: <service>_<action>
RE_IS_SERVICE = re.compile("^([^_]+)_[^_]+$")
# operation: <service>_<component>_<action>
RE_GET_SERVICE = re.compile("^([^_]+)_.*")
RE_GET_COMPONENT = re.compile("^[^_]+_(.*)_[^_]+$")
RE_GET_ACTION = re.compile(".*_([^_]+)$")


SERVICE_NAME_MAX_LENGTH = 20
COMPONENT_NAME_MAX_LENGTH = 30
ACTION_NAME_MAX_LENGTH = 20

NODE_NAME_MAX_LENGTH = (
    SERVICE_NAME_MAX_LENGTH + COMPONENT_NAME_MAX_LENGTH + ACTION_NAME_MAX_LENGTH
)


class Operation:
    def __init__(self, name, collection_name=None, depends_on=None, noop=False):
        self.name = name
        self.collection_name = collection_name
        self.depends_on = depends_on or []
        self.noop = noop

        if len(name) > NODE_NAME_MAX_LENGTH:
            raise ValueError(f"{name} is longer than {NODE_NAME_MAX_LENGTH}")

        match = RE_GET_SERVICE.search(self.name)
        if not match:
            raise ValueError(f"Fail to parse service name for component '{self.name}'")
        self.service = match.group(1)

        if len(self.service) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(
                f"service {self.service} is longer than {SERVICE_NAME_MAX_LENGTH}"
            )

        match = RE_GET_ACTION.search(self.name)
        if not match:
            raise ValueError(f"Fail to parse action name for component '{self.name}'")
        self.action = match.group(1)

        if len(self.action) > ACTION_NAME_MAX_LENGTH:
            raise ValueError(
                f"action {self.service} is longer than {ACTION_NAME_MAX_LENGTH}"
            )

        match = RE_GET_COMPONENT.search(self.name)
        if not match:
            self.component = None
        else:
            self.component = match.group(1)
        if (
            self.component is not None
            and len(self.component) > COMPONENT_NAME_MAX_LENGTH
        ):
            raise ValueError(
                f"component {self.component} is longer than {COMPONENT_NAME_MAX_LENGTH}"
            )

    def is_service(self):
        """Return True if the operation is about a service, False otherwise"""
        return bool(RE_IS_SERVICE.search(self.name))

    def __repr__(self):
        return (
            f"Operation(name={self.name}, "
            f"collection_name={self.collection_name}, "
            f"depends_on={self.depends_on}, "
            f"noop={self.noop})"
        )
