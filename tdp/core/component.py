# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import re

logger = logging.getLogger("tdp").getChild("component")

# service: <service>_<action>
RE_IS_SERVICE = re.compile("^([^_]+)_[^_]+$")
# component: <service>_<component>_<action>
RE_GET_SERVICE = re.compile("^([^_]+)_.*")
RE_GET_ACTION = re.compile(".*_([^_]+)$")

NODE_NAME_MAX_LENGTH = 50


class Component:
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

        match = RE_GET_ACTION.search(self.name)
        if not match:
            raise ValueError(f"Fail to parse action name for component '{self.name}'")
        self.action = match.group(1)

    def is_service(self):
        """Return True if the component is a service, False otherwise"""
        return bool(RE_IS_SERVICE.search(self.name))
