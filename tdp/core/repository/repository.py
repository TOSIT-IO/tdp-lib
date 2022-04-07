# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod, abstractstaticmethod
from collections import OrderedDict
from contextlib import ExitStack, contextmanager
from threading import RLock
from weakref import proxy

from tdp.core.variables import Variables

# Version string length isn't checked before inserting into database
VERSION_MAX_LENGTH = 40


class NoVersionYet(Exception):
    pass


class NotARepository(Exception):
    pass


class Repository(ABC):
    """Abstract class representing a versionned repository.

    A versionned repository is a folder where modification are stored and versionned.
    An implementation of this class is :py:class:`~tdp.core.repository.git_repository.GitRepository`.
    It uses Git as a versionning engine.
    """

    def __init__(self, path):
        self.path = path
        self._lock = RLock()

    def __enter__(self):
        self._lock.acquire()
        return proxy(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()

    @contextmanager
    def open_var_file(self, path):
        """Returns a Variables object managed, simplyfing use.

        Returns a Variables object automatically closed when parent context manager closes it.
        Adds the underlying file for validation.
        Args:
            path ([PathLike]): Path to open as a Variables file.

        Yields:
            [Proxy[Variables]]: A weakref of the Variables object, to prevent the creation of strong references
                outside the caller's context
        """
        with self._lock:
            path = self.path / path
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                path.touch()
            with Variables(path).open() as variables:
                yield variables
            self.add_for_validation(path)

    @contextmanager
    def open_var_files(self, paths):
        """Returns an OrderedDict of dict[path] = Variables object

        Args:
            paths ([List[PathLike]]): List of paths to open

        Yields:
            [OrderedDict[PathLike, Variables]]: Returns an OrderedDict where keys
                are sorted by the order of the input paths
        """
        with self._lock, ExitStack() as stack:
            yield OrderedDict(
                (path, stack.enter_context(self.open_var_file(path))) for path in paths
            )

    @abstractstaticmethod
    def init(path):
        pass

    @abstractmethod
    def add_for_validation(self, path):
        pass

    @abstractmethod
    @contextmanager  # type: ignore
    def validate(self, message):
        pass

    @abstractmethod
    def current_version(self):
        pass

    @abstractmethod
    def is_clean(self):
        pass

    @abstractmethod
    def files_modified(self, commit):
        pass
