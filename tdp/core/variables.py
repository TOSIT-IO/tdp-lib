import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from contextlib import contextmanager
from pathlib import Path

import ansible.constants as C


class Variables:
    """Manages group vars"""

    def __init__(self, group_vars_path):
        self._group_vars_path = Path(group_vars_path)

    @property
    def group_vars_path(self):
        return self._group_vars_path

    @contextmanager
    def _open_group(self, group):
        group_var_file_path = self.group_vars_path / group
        if not group_var_file_path.exists():
            group_var_file_path.touch()

        with open(group_var_file_path, "r+") as group_var_file:
            content = yaml.load(group_var_file, Loader=Loader) or {}

            yield content

            group_var_file.seek(0)
            group_var_file.write(yaml.dump(content, Dumper=Dumper))
            group_var_file.truncate()
            group_var_file.flush()

    def update(self, group, var):
        """

        Args:
            group (str): Ansible group that receive new variables
            var (dict): variables that will be written to the group vars
        """
        with self._open_group(group) as content:
            content.update(var)

    def unset(self, group, key):
        """[summary]

        Args:
            group ([type]): [description]
            key ([type]): key to delete (using dot notation for complexe keys)
        """
        with self._open_group(group) as content:
            subkeys = key.split(".")
            cursor = content
            for index, subkey in enumerate(subkeys):
                if cursor.get(subkey) and index < (len(subkeys) - 1):
                    cursor = cursor[subkey]
                else:
                    cursor.pop(".".join(subkeys[index:]))
                    break
