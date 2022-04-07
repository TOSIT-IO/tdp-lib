# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
import os
from contextlib import contextmanager
from functools import wraps
from weakref import proxy

import yaml

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper


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

    .. code-block:: python

        variables["key1"] = "value1" # set the value `value1` to the key `key1`
        value = variables["key1"] # get the value at the key `key1`
        del variables["key1"] # deletes value at key `key1`
    """

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
        """get a value matching the key in inner content.

        Returns a deepcopy of the value, in case a dict is returned and to prevent
        modification of the content outside of this class.  We don't support
        listening to the value of a shallow copy (yet).

        :param key: Key in inner content object (must be YAML compatible)
        :type key: Hashable
        :param default: Value to return if the key is absent from the inner content. Defaults to None.
        :type default: Any, optional
        :raises e: KeyError when key is missing and no default value has been provided
        :return: Value inside the inner content
        :rtype: Any
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

    def update(self, var, merge=True):
        """update

        Args:
            var (Union[dict, VariablesDict]): variables that will be written to the group vars
            merge (Bool): whether variables must be merged or overwritten
        """
        if isinstance(var, VariablesDict):
            updated_content = var._content
        else:
            updated_content = var

        if merge:
            self._content.update(updated_content)
        else:
            self._content = updated_content

    def unset(self, key):
        """unset key in variables, supports nested keys

        :param var: key to delete (using dot notation for complexe keys)
        :type var: str
        """
        subkeys = key.split(".")
        cursor = self._content
        for index, subkey in enumerate(subkeys):
            if cursor.get(subkey) and index < (len(subkeys) - 1):
                cursor = cursor[subkey]
            else:
                cursor.pop(".".join(subkeys[index:]))
                break

    def to_dict(self):
        return copy.deepcopy(self._content)


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

    def update(self, var, *args, **kwargs):
        with self._flush_on_write():
            super().update(var, *args, **kwargs)
