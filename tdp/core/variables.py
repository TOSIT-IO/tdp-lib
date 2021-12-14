import os
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pathlib import Path
from functools import wraps

import ansible.constants as C


class Variables:
    """Manages group vars"""

    def __init__(self, group_vars_path):
        self._group_vars_path = group_vars_path

    @property
    def group_vars_path(self):
        return self._group_vars_path

    def group_vars(f):
        """Decorator that insures the group var file exists"""

        @wraps(f)
        def wrapper(self, *args):
            group_var_file_path = Path(os.path.join(self.group_vars_path, args[0]))
            if not group_var_file_path.exists():
                group_var_file_path.touch()
            return f(self, *args)

        return wrapper

    @group_vars
    def update(self, group, var):
        """

        Args:
            group (str): Ansible group that receive new variables
            var (dict): variables that will be written to the group vars
        """
        with open(os.path.join(self.group_vars_path, group), "r+") as group_var_file:
            content = yaml.load(group_var_file, Loader=Loader) or {}

            content.update(var)

            group_var_file.seek(0)
            group_var_file.write(yaml.dump(content, Dumper=Dumper))
            group_var_file.truncate()
            group_var_file.flush()

    @group_vars
    def unset(self, group, key):
        """[summary]

        Args:
            group ([type]): [description]
            key ([type]): key to delete (using dot notation for complexe keys)
        """
        with open(os.path.join(self.group_vars_path, group), "r+") as group_var_file:
            group_var_file.seek(0)
            content = yaml.load(group_var_file, Loader=Loader) or {}

            subkeys = key.split(".")
            cursor = content
            for index, subkey in enumerate(subkeys):
                if cursor.get(subkey) and index < (len(subkeys) - 1):
                    cursor = cursor[subkey]
                else:
                    cursor.pop(".".join(subkeys[index:]))
                    break

            group_var_file.seek(0)
            group_var_file.write(yaml.dump(content, Dumper=Dumper))
            group_var_file.truncate()
            group_var_file.flush()
