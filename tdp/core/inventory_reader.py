# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Optional, TextIO

import yaml
from ansible.cli.inventory import InventoryCLI
from ansible.inventory.manager import InventoryManager

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader


# From ansible/cli/inventory.py
class _CustomInventoryCLI(InventoryCLI):
    """Represent a custom Ansible inventory CLI which does nothing.
    This is used to load inventory files with Ansible code.
    """

    def __init__(self):
        super().__init__(["program", "--list"])
        # "run" must be called from CLI (the parent of InventoryCLI), to
        # initialize context (reading ansible.cfg for example).
        super(InventoryCLI, self).run()
        # Get InventoryManager instance
        _, self.inventory, _ = self._play_prereqs()

    # Avoid call InventoryCLI "run", we do not need to run InventoryCLI
    def run(self):
        pass


custom_inventory_cli_instance = _CustomInventoryCLI()


class InventoryReader:
    """Represent an Ansible inventory reader."""

    def __init__(self, inventory: Optional[InventoryManager] = None):
        self.inventory = inventory or custom_inventory_cli_instance.inventory

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

    def get_hosts_from_playbook(self, fd: TextIO) -> set[str]:
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

        return hosts
