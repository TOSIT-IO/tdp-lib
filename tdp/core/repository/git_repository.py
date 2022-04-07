# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from contextlib import contextmanager

from git import InvalidGitRepositoryError, NoSuchPathError, Repo

from tdp.core.repository.repository import NotARepository, NoVersionYet, Repository

logger = logging.getLogger("tdp").getChild("git_repository")


class GitRepository(Repository):
    """
    `GitRepository` is an implementation for `Repository` which usee
    local Git repository to manage files with versionning.
    """

    def __init__(self, path):
        super().__init__(path)
        try:
            self._repo = Repo(self.path)
        except (InvalidGitRepositoryError, NoSuchPathError) as e:
            raise NotARepository(f"{self.path} is not a valid repository") from e

    def close(self):
        with self._lock:
            self._repo.close()

    @staticmethod
    def init(path):
        try:
            with Repo(path):
                return GitRepository(path)
        except (InvalidGitRepositoryError, NoSuchPathError):
            with Repo.init(path, mkdir=True):
                return GitRepository(path)

    @contextmanager
    def validate(self, msg):
        with self._lock:
            yield self
            commit = self._repo.index.commit(msg)
            logger.info(f"commit: [{commit.hexsha}] {msg}")

    def add_for_validation(self, path):
        with self._lock:
            self._repo.index.add([str(path)])
            logger.debug(f"{path} staged")

    def current_version(self):
        try:
            return str(self._repo.head.commit)
        except ValueError as e:
            raise NoVersionYet from e

    def is_clean(self):
        return not self._repo.is_dirty()

    def files_modified(self, commit):
        with self._lock:
            files = set()
            diff_index = self._repo.head.commit.diff(commit)
            for diff in diff_index:
                files.add(diff.a_path)
                files.add(diff.b_path)
            return list(files)
