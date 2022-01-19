import logging

from git import Repo

from tdp.core.repository import Repository


logger = logging.getLogger("tdp").getChild("git_repository")


class GitRepository(Repository):
    def __enter__(self):
        ref = super().__enter__()
        self._repo = Repo(self.path)
        return ref

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self._repo.close()

    def validate(self, msg):
        commit = self._repo.index.commit(msg)
        logger.info(f"commit: [{commit.hexsha}] {msg}")

    def add_for_validation(self, path):
        self._repo.index.add([str(path)])
        logger.debug(f"{path} staged")
