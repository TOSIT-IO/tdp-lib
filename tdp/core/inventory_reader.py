# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, TextIO

import yaml

from tdp.core.ansible_loader import AnsibleLoader

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

if TYPE_CHECKING:
    from ansible.inventory.manager import InventoryManager


class InventoryReader:
    """Represent an Ansible inventory reader."""

    def __init__(self, inventory: Optional[InventoryManager] = None):
        self.inventory = inventory or AnsibleLoader.get_CustomInventoryCLI().inventory

    def get_hosts(self, *args, **kwargs) -> list[str]:
        """Takes a pattern or list of patterns and returns a list of matching
        inventory host names, taking into account any active restrictions
        or applied subsets.

        See ansible.inventory.manager.InventoryManager class.

        Returns:
            List of hosts.
        """
        # "ansible.inventory.manager.InventoryManager.get_hosts()" returns a list of
        # "ansible.inventory.host.Host" instance and we need the name of each host
        # so we convert them to "str"
        return [str(host) for host in self.inventory.get_hosts(*args, **kwargs)]

    def get_hosts_from_playbook(self, fd: TextIO) -> frozenset[str]:
        """Takes a playbook content, read all plays inside and return a set
        of matching host like "ansible-playbook --list-hosts playbook.yml".

        Args:
            fd: file-like object from which the playbook content must be read.

        Returns:
            Set of hosts.
        """
        plays = yaml.load(fd, Loader=Loader)
        if not isinstance(plays, list):
            raise TypeError(f"Playbook content is not a list, given {type(plays)}")

        hosts: set[str] = set()

        for play in plays:
            if not isinstance(play, dict):
                raise TypeError(f"A play must be a dict, given {type(play)}")
            if "hosts" not in play:
                raise ValueError(
                    f"'hosts' key is mandatory for a play, keys are {play.keys()}"
                )
            hosts.update(self.get_hosts(play["hosts"]))

        return frozenset(hosts)
