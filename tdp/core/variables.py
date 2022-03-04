import copy
from functools import wraps
import logging
import os
from weakref import proxy
import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from contextlib import contextmanager


logger = logging.getLogger("tdp").getChild("variables")

# https://stackoverflow.com/a/33300001
def str_presenter(dumper, data):
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


Dumper.add_representer(str, str_presenter)


class Variables:
    """Manages a var file

    This object is meant to be short lived.
    """

    def __init__(self, file_path):
        self._file_path = file_path

    def open(self):
        return _VariablesIOWrapper(self._file_path)


class VariablesDict:
    """Manages internal content logic. Internal instanciation only.

    This object implements the getitem, setitem and delitem methods, allowing for:
    ```python
        variables["key1"] = "value1" # set the value `value1` to the key `key1`
        value = variables["key1] # get the value at the key `key1`
        del variables["key1"] # deletes value at key `key1`
    ```"""

    def __init__(self, content):
        """
        Args:
            content ([Dict]): Content of a var file
        """
        self._content = content

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        self.unset(key)

    def get(self, key, default=None):
        """Returns a copy of the value matching the key in inner content.

        Returns a deepcopy of the value, in case a dict is returned and to prevent
        modification of the content outside of this class.  We don't support
        listening to the value of a shallow copy (yet).
        Args:
            key ([Hashable]): Key in inner content object (must be YAML compatible)
            default ([Any], optional): Value to return if the key is absent from the inner content. Defaults to None.

        Raises:
            e: KeyError when key is missing and no default value has been provided

        Returns:
            [Any]: Value inside the inner content
        """
        subkeys = key.split(".")
        cursor = self._content
        try:
            for index, subkey in enumerate(subkeys):
                if cursor.get(subkey) and index < (len(subkeys) - 1):
                    cursor = cursor[subkey]
                else:
                    return copy.deepcopy(cursor.get(".".join(subkeys[index:])))
        except KeyError as e:
            if default:
                return default
            raise e

    def set(self, key, value):
        subkeys = key.split(".")
        cursor = self._content
        for index, subkey in enumerate(subkeys):
            if cursor.get(subkey) and index < (len(subkeys) - 1):
                cursor = cursor[subkey]
            else:
                cursor[".".join(subkeys[index:])] = value
                break

    def update(self, var):
        """update

        Args:
            var (Union[dict, VariablesDict]): variables that will be written to the group vars
        """
        if isinstance(var, VariablesDict):
            self._content.update(var._content)
        else:
            self._content.update(var)

    def unset(self, key):
        """[summary]

        Args:
            key ([type]): key to delete (using dot notation for complexe keys)
        """
        subkeys = key.split(".")
        cursor = self._content
        for index, subkey in enumerate(subkeys):
            if cursor.get(subkey) and index < (len(subkeys) - 1):
                cursor = cursor[subkey]
            else:
                cursor.pop(".".join(subkeys[index:]))
                break


class _VariablesIOWrapper(VariablesDict):
    def __init__(self, path):
        self._file_path = path
        self._file_descriptor = open(self._file_path, "r+")
        content = yaml.load(self._file_descriptor, Loader=Loader) or {}
        super().__init__(content)

    def __enter__(self):
        return proxy(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @contextmanager
    def _flush_on_write(self):
        if not self._file_descriptor or self._file_descriptor.closed:
            raise RuntimeError(
                f"{self._file_path} is already closed, which shouldn't happen"
            )
        yield

        self._file_descriptor.seek(0)
        self._file_descriptor.write(
            yaml.dump(self._content, Dumper=Dumper, sort_keys=False, width=1000)
        )
        self._file_descriptor.truncate()
        self._file_descriptor.flush()
        # https://docs.python.org/3/library/os.html#os.fsync
        os.fsync(self._file_descriptor.fileno())

    def close(self):
        if self._file_descriptor and not self._file_descriptor.closed:
            self._file_descriptor.close()
            self._variables_dict = None
        else:
            raise RuntimeError(
                f"{self._file_path} is already closed, which shouldn't happen"
            )

    def get(self, key, default=None):
        with self._flush_on_write():
            return super().get(key, default)

    def set(self, key, value):
        with self._flush_on_write():
            super().set(key, value)

    def unset(self, key):
        with self._flush_on_write():
            super().unset(key)

    def update(self, var):
        with self._flush_on_write():
            super().update(var)
