import logging
import re

logger = logging.getLogger("tdp").getChild("component")

# service: <service>_<action>
RE_IS_SERVICE = re.compile("^([^_]+)_[^_]+$")
# component: <service>_<component>_<action>
RE_GET_SERVICE = re.compile("^([^_]+)_.*")
RE_GET_ACTION = re.compile(".*_([^_]+)$")


class Component:
    def __init__(self, name, depends_on=None, noop=False):
        self.name = name
        self.depends_on = depends_on or []
        self.noop = noop

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
