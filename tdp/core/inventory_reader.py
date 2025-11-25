# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from tdp.core.ansible_loader import AnsibleLoader
from tdp.core.collections.playbook_validate import PlaybookIn

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

    def get_hosts_from_playbook(self, playbook: PlaybookIn) -> frozenset[str]:
        """Takes a playbook content, read all plays inside and return a set
        of matching host like "ansible-playbook --list-hosts playbook.yml".
        """
        hosts: set[str] = set()

        for play in playbook:
            hosts.update(self.get_hosts(play.hosts))

        return frozenset(hosts)
