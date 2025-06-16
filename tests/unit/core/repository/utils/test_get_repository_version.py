# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import pathlib

import pytest

from tdp.core.repository.repository import NoVersionYet
from tdp.core.repository.utils.get_repository_version import get_repository_version


def test_get_repository_version_no_repo(tmp_path):
    """Test with no repository."""
    msg = get_repository_version(tmp_path)

    assert msg == None


class FakeCleanRepo:
    def __init__(self, path: pathlib.Path):
        self.path = path

    def is_clean(self) -> bool:
        return True

    def current_version(self) -> str:
        return "v1.0"


class FakeDirtyRepo:
    def __init__(self, path: pathlib.Path):
        self.path = path

    def is_clean(self) -> bool:
        return False

    def current_version(self) -> str:
        return "v2.0"


class FakeNoVersionRepo:
    def __init__(self, path: pathlib.Path):
        self.path = path

    def is_clean(self) -> bool:
        return True

    def current_version(self) -> str:
        raise NoVersionYet()


@pytest.mark.parametrize(
    "RepoClass, expect_warning, expected_version, warning_substr",
    [
        (FakeCleanRepo, False, "v1.0", None),
        (FakeDirtyRepo, True, "v2.0", "is a repository but is not clean"),
        (FakeNoVersionRepo, True, "No version yet", "has no version yet"),
    ],
)
def test_get_repository_version_repo_variants(
    tmp_path, caplog, RepoClass, expect_warning, expected_version, warning_substr
):
    """Test clean, dirty, and no-version repository scenarios."""
    msg = get_repository_version(tmp_path, repository_class=RepoClass)

    assert msg == expected_version

    if expect_warning:
        # Exactly one warning expected
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(warning_substr in str(w) for w in warnings)
    else:
        # No warnings for clean repo
        assert not any(r.levelno == logging.WARNING for r in caplog.records)
