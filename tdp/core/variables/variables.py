# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from collections.abc import MutableMapping
from weakref import proxy

import jsonschema
import yaml
from ansible.utils.vars import merge_hash as _merge_hash

try:
    from yaml import CDumper as Dumper
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Dumper, Loader


logger = logging.getLogger("tdp").getChild("variables")

# https://stackoverflow.com/a/33300001
def str_presenter(dumper, data):
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


Dumper.add_representer(str, str_presenter)


def merge_hash(dict_a, dict_b):
    return _merge_hash(dict_a, dict_b)


def is_object(checker, instance):
    return jsonschema.Draft7Validator.TYPE_CHECKER.is_type(
        instance, "object"
    ) or isinstance(instance, (VariablesDict, _VariablesIOWrapper))


class Variables:
    """Manages a var file

    This object is meant to be short lived.
    """

    def __init__(self, file_path):
        self._file_path = file_path

    def open(self, mode=None):
        return _VariablesIOWrapper(self._file_path, mode)


class VariablesDict(MutableMapping):
    """Manages internal content logic. Internal instanciation only.

    This object implements the getitem, setitem and delitem methods, allowing for:

    .. code-block:: python

        variables["key1"] = "value1" # set the value `value1` to the key `key1`
        value = variables["key1"] # get the value at the key `key1`
        del variables["key1"] # deletes value at key `key1`
    """

    def __init__(self, content, name=None):
        """
        Args:
            content ([Dict]): Content of a var file
        """
        self._content = content
        self._name = name

    @property
    def name(self):
        return self._name

    def copy(self):
        return self._content.copy()

    def merge(self, mapping):
        self._content = merge_hash(self._content, mapping)

    def __getitem__(self, key):
        return self._content.__getitem__(key)

    def __setitem__(self, key, value):
        return self._content.__setitem__(key, value)

    def __delitem__(self, key):
        return self._content.__delitem__(key)

    def __len__(self) -> int:
        return self._content.__len__()

    def __iter__(self):
        return self._content.__iter__()


class _VariablesIOWrapper(VariablesDict):
    def __init__(self, path, mode=None):
        self._file_path = path
        self._file_descriptor = open(self._file_path, mode or "r+")
        self._content = yaml.load(self._file_descriptor, Loader=Loader) or {}
        self._name = path.name

    def __enter__(self):
        return proxy(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _flush_on_disk(self):
        if not self._file_descriptor or self._file_descriptor.closed:
            raise RuntimeError(
                f"{self._file_path} is already closed, which shouldn't happen"
            )

        if not self._file_descriptor.writable():
            return

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
            self._flush_on_disk()
            self._file_descriptor.close()
            self._content = None
        else:
            raise RuntimeError(
                f"{self._file_path} is already closed, which shouldn't happen"
            )
