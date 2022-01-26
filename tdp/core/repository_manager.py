from contextlib import contextmanager
from pathlib import Path
from threading import Lock

from tdp.core.git_repository import GitRepository


class RepositoryManager:
    """RepositoryManager is meant to be used as a single instance shared across threads."""

    def __init__(self, path, repository_class=GitRepository):
        self.path = Path(path)
        self._repository_class = repository_class
        self._lock = Lock()

    def init(self):
        with self._lock:
            self._repository_class(self.path).init()

    @contextmanager
    def validate(self, message):
        with self._lock, self._repository_class(self.path) as repository:
            yield repository
            repository.validate(message)
