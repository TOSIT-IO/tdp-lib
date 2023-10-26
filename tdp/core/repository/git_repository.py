# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager

from git import BadName, InvalidGitRepositoryError, NoSuchPathError, Repo

from tdp.core.repository.repository import (
    EmptyCommit,
    NotARepository,
    NoVersionYet,
    Repository,
)
from tdp.core.types import PathLike

logger = logging.getLogger(__name__)


class GitRepository(Repository):
    """Local Git repository to manage files with versionning."""

    def __init__(self, path: PathLike):
        super().__init__(path)
        try:
            self._repo = Repo(self.path)
        except (InvalidGitRepositoryError, NoSuchPathError) as e:
            raise NotARepository(f"{self.path} is not a valid repository") from e

    def close(self) -> None:
        with self._lock:
            self._repo.close()

    @staticmethod
    def init(path: PathLike) -> GitRepository:
        """Initialize a new Git repository at the given path."""
        try:
            with Repo(path):
                logger.debug(f"{path} is already a git repository")
                return GitRepository(path)
        except (InvalidGitRepositoryError, NoSuchPathError):
            with Repo.init(path, mkdir=True):
                logger.debug(f"git repository initialized at {path}")
                return GitRepository(path)

    @contextmanager
    def validate(self, msg: str) -> Generator[GitRepository, None, None]:
        with self._lock:
            yield self
            commit = None
            try:
                if len(self._repo.heads) == 0:
                    # Empty repository
                    commit = self._repo.index.commit(msg)
                    logger.info(f"initial commit: [{commit.hexsha}] {msg}")
                else:
                    if len(self._repo.index.diff("HEAD")) == 0:
                        raise EmptyCommit(
                            "validating these changes would produce no difference"
                        )
                    commit = self._repo.index.commit(msg)
                    logger.info(f"commit: [{commit.hexsha}] {msg}")
            except BadName as e:
                logger.debug(f"error during diff: {e}.")

    def add_for_validation(self, paths: list[PathLike]) -> None:
        with self._lock:
            self._repo.index.add(paths)
            logger.debug(f"{', '.join([str(p) for p in paths])} staged")

    def current_version(self) -> str:
        try:
            return str(self._repo.head.commit)
        except ValueError as e:
            raise NoVersionYet from e

    def is_clean(self) -> bool:
        return not self._repo.is_dirty(untracked_files=True)

    def is_file_modified(self, commit: str, path: PathLike) -> bool:
        with self._lock:
            diff_index = self._repo.head.commit.diff(commit)
            for diff in diff_index:
                if diff.a_path == path or diff.b_path == path:
                    return True
            return False

    def restore_file(self, file_names: str) -> None:
        self._repo.index.checkout(paths=[file_names], force=True)
