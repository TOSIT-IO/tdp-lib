from contextlib import contextmanager
import logging

from git import Repo, InvalidGitRepositoryError

from tdp.core.repository.repository import Repository


logger = logging.getLogger("tdp").getChild("git_repository")


class GitRepository(Repository):
    def __init__(self, path):
        super().__init__(path)
        self._repo = Repo(self.path)

    def __enter__(self):
        return super().__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self.close()

    def close(self):
        with self._lock:
            self._repo.close()

    @staticmethod
    def init(path):
        try:
            with Repo(path):
                pass
        except InvalidGitRepositoryError:
            with Repo.init(path, mkdir=True):
                pass

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
