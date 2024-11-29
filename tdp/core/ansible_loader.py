# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


class AnsibleLoader:
    """Lazy loader for Ansible classes and functions.

    This class is required as ansible automatically generate a config when imported.
    """

    _merge_hash = None
    _from_yaml = None
    _AnsibleDumper = None
    _InventoryCLI = None
    _InventoryReader = None
    _InventoryManager = None
    _CustomInventoryCLI = None

    @classmethod
    def load_merge_hash(cls):
        """Load the merge_hash function from ansible."""
        if cls._merge_hash is None:
            from ansible.utils.vars import merge_hash

            cls._merge_hash = merge_hash

        return cls._merge_hash

    @classmethod
    def load_from_yaml(cls):
        """Load the from_yaml function from ansible."""
        if cls._from_yaml is None:
            from ansible.parsing.utils.yaml import from_yaml

            cls._from_yaml = from_yaml

        return cls._from_yaml

    @classmethod
    def load_AnsibleDumper(cls):
        """Load the AnsibleDumper class from ansible."""
        if cls._AnsibleDumper is None:
            from ansible.parsing.yaml.dumper import AnsibleDumper

            cls._AnsibleDumper = AnsibleDumper

        return cls._AnsibleDumper

    @classmethod
    def load_InventoryCLI(cls):
        """Load the InventoryCLI class from ansible."""
        if cls._InventoryCLI is None:
            from ansible.cli.inventory import InventoryCLI

            cls._InventoryCLI = InventoryCLI

        return cls._InventoryCLI

    @classmethod
    def load_InventoryReader(cls):
        """Load the InventoryReader class from ansible."""
        if cls._InventoryReader is None:
            from tdp.core.inventory_reader import InventoryReader

            cls._InventoryReader = InventoryReader

        return cls._InventoryReader

    @classmethod
    def load_InventoryManager(cls):
        """Load the InventoryManager class from ansible."""
        if cls._InventoryManager is None:
            from ansible.inventory.manager import InventoryManager

            cls._InventoryManager = InventoryManager

        return cls._InventoryManager

    @classmethod
    def get_CustomInventoryCLI(cls):
        if cls._CustomInventoryCLI is None:

            class CustomInventoryCLI(cls.load_InventoryCLI()):
                """Represent a custom Ansible inventory CLI which does nothing.
                This is used to load inventory files with Ansible code.
                """

                def __init__(self):
                    super().__init__(["program", "--list"])
                    # "run" must be called from CLI (the parent of InventoryCLI), to
                    # initialize context (reading ansible.cfg for example).
                    super(cls.load_InventoryCLI(), self).run()
                    # Get InventoryManager instance
                    _, self.inventory, _ = self._play_prereqs()

                # Avoid call InventoryCLI "run", we do not need to run InventoryCLI
                def run(self):
                    pass

            cls._CustomInventoryCLI = CustomInventoryCLI()

        return cls._CustomInventoryCLI
