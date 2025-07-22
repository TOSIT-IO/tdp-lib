# Copyright 2025 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


class AnsibleLoader:
    """Lazy loader for Ansible packages."""

    _merge_hash = None
    _from_yaml = None
    _AnsibleDumper = None
    _InventoryCLI = None
    _InventoryReader = None
    _CustomInventoryCLI = None

    @classmethod
    def get_merge_hash(cls):
        """Get the merge_hash function from ansible.utils.vars."""
        if cls._merge_hash is None:
            from ansible.utils.vars import merge_hash

            cls._merge_hash = merge_hash
        return cls._merge_hash

    @classmethod
    def get_from_yaml(cls):
        """Get the from_yaml function from ansible.utils.vars."""
        if cls._from_yaml is None:
            from ansible.parsing.utils.yaml import from_yaml

            cls._from_yaml = from_yaml
        return cls._from_yaml

    @classmethod
    def get_AnsibleDumper(cls):
        """Get the AnsibleDumper class from ansible.utils.vars."""
        if cls._AnsibleDumper is None:
            from ansible.parsing.yaml.dumper import AnsibleDumper

            cls._AnsibleDumper = AnsibleDumper
        return cls._AnsibleDumper

    @classmethod
    def get_InventoryCLI(cls):
        """Get the InventoryCLI class from ansible.cli.inventory."""
        if cls._InventoryCLI is None:
            from ansible.cli.inventory import InventoryCLI

            cls._InventoryCLI = InventoryCLI
        return cls._InventoryCLI

    @classmethod
    def get_CustomInventoryCLI(cls):
        """Get the CustomInventoryCLI class."""

        class _CustomInventoryCLI(cls.get_InventoryCLI()):
            """Represent a custom Ansible inventory CLI which does nothing.
            This is used to load inventory files with Ansible code.
            """

            def __init__(self):
                super().__init__(["program", "--list"])
                # "run" must be called from CLI (the parent of InventoryCLI), to
                # initialize context (reading ansible.cfg for example).
                super(cls.get_InventoryCLI(), self).run()
                # Get InventoryManager instance
                _, self.inventory, _ = self._play_prereqs()

            # Avoid call InventoryCLI "run", we do not need to run InventoryCLI
            def run(self):
                pass

        if cls._CustomInventoryCLI is None:
            cls._CustomInventoryCLI = _CustomInventoryCLI()
        return cls._CustomInventoryCLI
