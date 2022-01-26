import logging

from git import Repo, InvalidGitRepositoryError

from tdp.core.repository import Repository


logger = logging.getLogger("tdp").getChild("git_repository")


class GitRepository(Repository):
    def __init__(self, path):
        super().__init__(path)

    def __enter__(self):
        ref = super().__enter__()
        self._repo = Repo(self.path)
        return ref

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        self._repo.close()

    def init(self):
        try:
            Repo(self.path).close()
        except InvalidGitRepositoryError:
            Repo.init(self.path, mkdir=True).close()

    def validate(self, msg):
        commit = self._repo.index.commit(msg)
        logger.info(f"commit: [{commit.hexsha}] {msg}")

    def add_for_validation(self, path):
        self._repo.index.add([str(path)])
        logger.debug(f"{path} staged")
