# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from pathlib import Path
from typing import Optional

from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import NotARepository, NoVersionYet, Repository

logger = logging.getLogger(__name__)


def get_repository_version(
    path: Path, *, repository_class: type[Repository] = GitRepository
) -> Optional[str]:
    """Returns the repository's version if the path is a repository."""
    try:
        repo = repository_class(path)
    except NotARepository:
        return None
    if not repo.is_clean():
        logger.warning(f"{path} is a repository but is not clean.")
    try:
        return repo.current_version()
    except NoVersionYet:
        logger.warning(f"{path} is a repository but has no version yet.")
        return "No version yet"
