# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from pathlib import Path
from threading import RLock
from weakref import proxy

from tdp.core.types import PathLike

# Version string length isn't checked before inserting into database
VERSION_MAX_LENGTH = 40


class NoVersionYet(Exception):
    """Raised when trying to get the version of a repository that has no version yet."""

    pass


class NotARepository(Exception):
    """Raised when trying to use a folder that is not a repository."""

    pass


class EmptyCommit(Exception):
    """Raised when trying to validate an empty commit."""

    pass


class Repository(ABC):
    """Abstract class representing a versionned repository.

    A versionned repository is a folder where modification are stored and versionned.
    An implementation of this class is :py:class:`~tdp.core.repository.git_repository.GitRepository`.
    It uses Git as a versionning engine.
    """

    def __init__(self, path: PathLike):
        """Initialize a Repository instance.

        Args:
            path: Path to the repository.

        Raises:
            NotARepository: If the path is not a valid repository.
        """
        self.path = Path(path)
        self._lock = RLock()

    def __enter__(self):
        self._lock.acquire()
        return proxy(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()

    @classmethod
    @abstractmethod
    def init(cls, path: PathLike) -> Repository:
        """Initialize a new repository.

        Args:
            path: Path to the repository.

        Returns:
            A repository object.
        """

    @abstractmethod
    def add_for_validation(self, paths: Iterable[PathLike]) -> None:
        """Add files to the index for validation.

        Args:
            paths: List of paths to add.
        """
        pass

    @abstractmethod
    @contextmanager
    def validate(self, message: str) -> Generator[Repository, None, None]:
        """Validate the changes in the index.

        Args:
            message: Validation message.

        Returns:
            A repository object.

        Raises:
            EmptyCommit: If the commit would be empty.
        """
        pass

    @abstractmethod
    def current_version(self) -> str:
        """Get the current version of the repository.

        Returns:
            The current version.

        Raises:
            NoVersionYet: If the repository is empty.
        """
        pass

    @abstractmethod
    def is_clean(self) -> bool:
        """Check if the repository is clean.

        Returns:
            True if the repository is clean, False otherwise.
        """
        pass

    @abstractmethod
    def is_file_modified(self, commit: str, path: PathLike) -> bool:
        """Check if a file has been modified in a commit.

        Args:
            commit: Commit hash.
            path: Path to the file.

        Returns:
            True if the file has been modified, False otherwise.
        """
        pass

    @abstractmethod
    def restore_file(self, file_names: str) -> None:
        """Cancel the file_name modifications back to the last commit.

        Args:
            file_name: file name to cancel.

        Returns:
            None.
        """
        pass
