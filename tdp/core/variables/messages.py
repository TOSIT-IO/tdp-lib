# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tdp.core.constants import VALIDATION_MESSAGE_FILE
from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import Repository
from tdp.core.repository.utils.get_repository_version import get_repository_version

if TYPE_CHECKING:
    from tdp.core.collections.collections import Collections


class ValidationMessageBuilder:
    """Builds validation messages for collection and override service sources."""

    def __init__(
        self,
        collections: Collections,
        repository_class: type[Repository] = GitRepository,
        validation_msg_file_name: str = VALIDATION_MESSAGE_FILE,
    ):
        self.collections = collections
        self.repository_class = repository_class
        self.validation_msg_file_name = validation_msg_file_name

    def for_collection(self, collection_name: str) -> str:
        msg = [
            f"Update variables from collection: {collection_name}",
            f"Path: {self.collections.default_vars_dirs[collection_name].as_posix()}",
        ]
        versions = self.collections.get_version(collection_name)
        if versions.galaxy:
            msg.append(f"Galaxy collection version: {versions.galaxy}")
        if versions.repo:
            msg.append(f"Repository version: {versions.repo}")
        return "\n".join(msg)

    def for_override(self, override_path: Path) -> str:
        msg = [f"Update variables from override: {override_path.as_posix()}"]
        if repo_version := get_repository_version(
            override_path, repository_class=self.repository_class
        ):
            msg.append(f"Repository version: {repo_version}")
        return "\n".join(msg)

    def for_service(self, service_path: Path) -> Optional[str]:
        validation_file = service_path / self.validation_msg_file_name
        try:
            return validation_file.read_text().strip()
        except (PermissionError, FileNotFoundError, NotADirectoryError):
            return None
