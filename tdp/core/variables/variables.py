# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from collections.abc import MutableMapping
from pathlib import Path
from typing import Optional
from weakref import proxy

import yaml
from ansible.parsing.utils.yaml import from_yaml
from ansible.parsing.yaml.dumper import AnsibleDumper
from ansible.utils.vars import merge_hash

from tdp.core.types import PathLike


class Variables:
    """Manages a variables file.

    This object is meant to be short lived. It should be used as follows:

        with Variables("path/to/file").open([mode]) as variables:
            variables["key1"] = "value1" # set the value `value1` to the key `key1`
            value = variables["key1"] # get the value at the key `key1`
            del variables["key1"] # deletes value at key `key1`
    """

    def __init__(self, file_path: PathLike, /, *, create_if_missing: bool = False):
        """Initializes a Variables instance.

        Args:
            file_path: Path to the file.
            create_if_missing: Whether to create the file if it does not exist.
        """
        self._file_path = Path(file_path)
        # Create the file if it does not exist
        if not self._file_path.exists():
            if not create_if_missing:
                raise FileNotFoundError(f"'{file_path}' does not exist.")
            self._file_path.parent.mkdir(parents=True, exist_ok=True)
            self._file_path.touch()

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

    def __init__(self, content: dict, name: Optional[str] = None):
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

    def copy(self) -> dict:
        """Copy the content of the variables file.

        Returns:
            Copy of the content.
        """
        return self._content.copy()

    def merge(self, mapping: MutableMapping) -> None:
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
        self._content = from_yaml(self._file_descriptor) or {}
        self._name = path.name

    def __enter__(self) -> "_VariablesIOWrapper":
        return proxy(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _flush_on_disk(self) -> None:
        """Write the content of the variables file on disk.

        Raises:
            RuntimeError: If the file descriptor is already closed.
        """
        # Check if the file descriptor is already closed
        if not self._file_descriptor or self._file_descriptor.closed:
            raise RuntimeError(
                f"{self._file_path} is already closed, which shouldn't happen"
            )

        # Check if the file is writable
        if not self._file_descriptor.writable():
            raise RuntimeError(f"{self._file_path} is not writable")

        # Write the content of the variables file on disk
        self._file_descriptor.seek(0)
        self._file_descriptor.write(
            yaml.dump(self._content, Dumper=AnsibleDumper, sort_keys=False, width=1000)
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
        # Check if the file descriptor is already closed
        if not self._file_descriptor or self._file_descriptor.closed:
            raise RuntimeError(
                f"{self._file_path} is already closed, which shouldn't happen"
            )

        # Flush to disk only if the file is writable
        if self._file_descriptor.writable():
            self._flush_on_disk()

        # Close the file descriptor
        self._file_descriptor.close()
        self._content = None
