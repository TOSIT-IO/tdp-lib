# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from collections.abc import MutableMapping
from pathlib import Path
from typing import Any, Dict, Optional, Union
from weakref import proxy

import jsonschema
import yaml
from ansible.utils.vars import merge_hash as _merge_hash

try:
    from yaml import CDumper as Dumper
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Dumper, Loader


# https://stackoverflow.com/a/33300001
def str_presenter(dumper: Dumper, data: str) -> str:
    r"""Presents a multiline string in a YAML-friendly format.

    Args:
        dumper: YAML dumper.
        data: String to present.

    Returns:
        YAML-friendly string representation.

    Examples:
        >>> str_presenter("foo\nbar")
        '"foo\\nbar"\\n'
        >>> str_presenter("foo\nbar", Dumper=Dumper)
        '|\n  foo\n  bar\n'
    """
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


Dumper.add_representer(str, str_presenter)


def merge_hash(dict_a: Dict, dict_b: Dict) -> Dict:
    """Merges two dictionaries.

    Args:
        dict_a: First dictionary.
        dict_b: Second dictionary.

    Returns:
        Merged dictionary.
    """
    return _merge_hash(dict_a, dict_b)


def is_object(checker: jsonschema.Draft7Validator, instance: Any) -> bool:
    """Custom type checker for JSON Schema.

    Args:
        checker: JSON Schema validator.
        instance: Instance to check.

    Returns:
        True if instance is an object or a VariablesDict/_VariablesIOWrapper instance, False otherwise.
    """
    return jsonschema.Draft7Validator.TYPE_CHECKER.is_type(
        instance, "object"
    ) or isinstance(instance, (VariablesDict, _VariablesIOWrapper))


class Variables:
    """Manages a variables file.

    This object is meant to be short lived. It should be used as follows:

        with Variables("path/to/file").open([mode]) as variables:
            variables["key1"] = "value1" # set the value `value1` to the key `key1`
            value = variables["key1"] # get the value at the key `key1`
            del variables["key1"] # deletes value at key `key1`
    """

    def __init__(self, file_path: Union[str, os.PathLike]):
        """Initializes a Variables instance.

        Args:
            file_path: Path to the file.
        """
        self._file_path = Path(file_path)

    def open(self, mode: Optional[str] = None) -> "_VariablesIOWrapper":
        """Opens the file in the given mode.

        Args:
            mode: Mode to open the file in.

        Returns:
            Wrapper for file IO operations.
        """
        return _VariablesIOWrapper(self._file_path, mode)


class VariablesDict(MutableMapping):
    """Variables file content.

    Manages the content of a variables file as a dictionary.
    """

    def __init__(self, content: Dict, name: Optional[str] = None):
        """Initializes the VariablesDict instance.

        Args:
            content: Content of a variables file.
            name: Name of the variables file. Defaults to None.
        """
        self._content = content
        self._name = name

    @property
    def name(self) -> Optional[str]:
        """Name of the variables file."""
        return self._name

    def copy(self) -> Dict:
        """Copy the content of the variables file.

        Returns:
            Copy of the content.
        """
        return self._content.copy()

    def merge(self, mapping: Dict) -> None:
        """Merges the provided mapping into the content.

        Args:
            mapping: Mapping to merge.
        """
        self._content = merge_hash(self._content, mapping)

    def __getitem__(self, key):
        return self._content.__getitem__(key)

    def __setitem__(self, key, value):
        return self._content.__setitem__(key, value)

    def __delitem__(self, key):
        return self._content.__delitem__(key)

    def __len__(self):
        return self._content.__len__()

    def __iter__(self):
        return self._content.__iter__()


class _VariablesIOWrapper(VariablesDict):
    """Context manager for file IO operations."""

    def __init__(self, path: Path, mode: Optional[str] = None):
        """Initializes the _VariablesIOWrapper instance.

        Args:
            path: Path to the file.
            mode: Mode to open the file in.
        """
        self._file_path = path
        self._file_descriptor = open(self._file_path, mode or "r+")
        self._content = yaml.load(self._file_descriptor, Loader=Loader) or {}
        self._name = path.name

    def __enter__(self):
        return proxy(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _flush_on_disk(self) -> None:
        """Write the content of the variables file on disk.

        Raises:
            RuntimeError: If the file descriptor is already closed.
        """
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

    def close(self) -> None:
        """Closes the file descriptor.

        Raises:
            RuntimeError: If the file descriptor is already closed.
        """
        if self._file_descriptor and not self._file_descriptor.closed:
            self._flush_on_disk()
            self._file_descriptor.close()
            self._content = None
        else:
            raise RuntimeError(
                f"{self._file_path} is already closed, which shouldn't happen"
            )
