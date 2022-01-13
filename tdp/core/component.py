import logging

logger = logging.getLogger("tdp").getChild("component")


class Component:
    def __init__(self, name, depends_on=None, noop=False):
        self.name = name
        self.depends_on = depends_on or []
        self.noop = noop
