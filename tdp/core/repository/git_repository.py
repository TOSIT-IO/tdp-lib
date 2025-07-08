# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import functools
import logging
from collections.abc import Generator, Iterable
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


def with_repo_path(func):
    """Decorator to annotate exceptions with the repository path."""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            raise type(e)(f"[Repo: {self.path}] {e}").with_traceback(e.__traceback__)

    return wrapper


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
                return GitRepository(path)
        except (InvalidGitRepositoryError, NoSuchPathError):
            with Repo.init(path, mkdir=True):
                return GitRepository(path)

    @with_repo_path
    @contextmanager
    def validate(self, msg: str) -> Generator[GitRepository, None, None]:
        with self._lock:
            yield self
            try:
                if len(self._repo.index.diff("HEAD")) == 0:
                    raise EmptyCommit(
                        "validating these changes would produce no difference"
                    )
            except BadName as e:
                logger.debug(
                    f"error during diff: {e}. Probably because the repo is still empty."
                )
            commit = self._repo.index.commit(msg)
            logger.info(f"commit: [{commit.hexsha}] {msg}")

    @with_repo_path
    def add_for_validation(self, paths: Iterable[PathLike]) -> None:
        logger.debug(paths)
        logger.debug(list(paths))
        with self._lock:
            self._repo.index.add(list(paths))
            logger.debug(f"{', '.join([str(p) for p in paths])} staged")

    @with_repo_path
    def current_version(self) -> str:
        try:
            return str(self._repo.head.commit)
        except ValueError as e:
            raise NoVersionYet from e

    @with_repo_path
    def is_clean(self) -> bool:
        return not self._repo.is_dirty(untracked_files=True)

    @with_repo_path
    def is_file_modified(self, commit: str, path: PathLike) -> bool:
        with self._lock:
            diff_index = self._repo.head.commit.diff(commit)
            for diff in diff_index:
                if diff.a_path == path or diff.b_path == path:
                    return True
            return False

    @with_repo_path
    def restore_file(self, file_names: str) -> None:
        self._repo.index.checkout(paths=[file_names], force=True)
