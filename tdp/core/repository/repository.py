# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod, abstractstaticmethod
from contextlib import contextmanager
from threading import RLock
from weakref import proxy

# Version string length isn't checked before inserting into database
VERSION_MAX_LENGTH = 40


class NoVersionYet(Exception):
    pass


class NotARepository(Exception):
    pass


class EmptyCommit(Exception):
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

    @abstractstaticmethod
    def init(path):
        pass

    @abstractmethod
    def add_for_validation(self, paths):
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
