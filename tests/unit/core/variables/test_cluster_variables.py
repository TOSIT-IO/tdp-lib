# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import pathlib
from typing import cast
from unittest.mock import MagicMock

import pytest

from tdp.core.collections.collection_reader import CollectionReader
from tdp.core.collections.collections import Collections
from tdp.core.repository.repository import NoVersionYet, Repository
from tdp.core.variables.cluster_variables import (
    VALIDATION_MESSAGE_FILE,
    ClusterVariables,
)

# Tests for ClusterVariables._get_service_custom_validation_msg


def test_service_custom_validation_msg_returns_custom_message(tmp_path):
    """Should return the custom message from the validation message file if present."""
    service_dir = tmp_path / "service"
    service_dir.mkdir()
    msg_file = service_dir / VALIDATION_MESSAGE_FILE
    custom_msg = "Custom validation message"
    msg_file.write_text(custom_msg)
    cv = ClusterVariables({}, MagicMock(spec=Collections))
    result = cv._get_service_custom_validation_msg(service_dir)
    assert result == custom_msg


def test_service_custom_validation_msg_returns_none_when_file_missing(tmp_path):
    """Should return None if the validation message file does not exist."""
    service_dir = tmp_path / "service"
    service_dir.mkdir()
    cv = ClusterVariables({}, MagicMock(spec=Collections))
    result = cv._get_service_custom_validation_msg(service_dir)
    assert result is None


def test_service_custom_validation_msg_with_custom_filename(tmp_path):
    """Should return the custom message from a custom-named validation message file."""
    service_dir = tmp_path / "service"
    service_dir.mkdir()
    custom_file = service_dir / "MY_MSG"
    custom_msg = "Another message"
    custom_file.write_text(custom_msg)
    cv = ClusterVariables({}, MagicMock(spec=Collections))
    result = cv._get_service_custom_validation_msg(
        service_dir, validation_msg_file_name="MY_MSG"
    )
    assert result == custom_msg


def test_service_custom_validation_msg_strips_whitespace(tmp_path):
    """Should strip whitespace from the validation message file content."""
    service_dir = tmp_path / "service"
    service_dir.mkdir()
    msg_file = service_dir / VALIDATION_MESSAGE_FILE
    msg_file.write_text("  message with spaces  \n")
    cv = ClusterVariables({}, MagicMock(spec=Collections))
    result = cv._get_service_custom_validation_msg(service_dir)
    assert result == "message with spaces"


def test_service_custom_validation_msg_file_not_readable(tmp_path):
    """Should return None or raise IOError if the validation message file is not readable."""
    service_dir = tmp_path / "service"
    service_dir.mkdir()
    msg_file = service_dir / VALIDATION_MESSAGE_FILE
    msg_file.write_text("Should not be readable")
    msg_file.chmod(0o000)
    cv = ClusterVariables({}, MagicMock(spec=Collections))
    try:
        result = cv._get_service_custom_validation_msg(service_dir)
        assert result is None
    except Exception as e:
        # Ensure the file is not readable, which should raise an IOError
        assert isinstance(e, IOError)
    finally:
        msg_file.chmod(0o644)  # Restore permissions for cleanup


def test_service_custom_validation_msg_service_path_is_file(tmp_path):
    """Should return None if the service path is a file, not a directory."""
    service_file = tmp_path / "service"
    service_file.write_text("not a directory")
    cv = ClusterVariables({}, MagicMock(spec=Collections))
    result = cv._get_service_custom_validation_msg(service_file)
    assert result is None


def test_service_custom_validation_msg_symlink_file(tmp_path):
    """Should return the message from a symlinked validation message file."""
    service_dir = tmp_path / "service"
    service_dir.mkdir()
    real_file = service_dir / "real_msg"
    real_file.write_text("Symlinked message")
    symlink_file = service_dir / VALIDATION_MESSAGE_FILE
    symlink_file.symlink_to(real_file)
    cv = ClusterVariables({}, MagicMock(spec=Collections))
    result = cv._get_service_custom_validation_msg(service_dir)
    assert result == "Symlinked message"


# Tests for ClusterVariables._get_override_base_validation_msg


def test_override_base_validation_msg_no_repo_override(tmp_path):
    """If repository_class returns None, only the base message should appear."""
    override_path = tmp_path / "override"
    override_path.mkdir()

    collections = MagicMock(default_vars_dirs={}, _collection_readers=[])
    cv = ClusterVariables({}, collections)
    # dummy_repo_class is a repository_class always returns None
    dummy_repo_class = cast(type[Repository], lambda x: None)
    msg = cv._get_override_base_validation_msg(
        override_path, repository_class=dummy_repo_class
    )

    expected = [f"Update variables from override: {override_path.as_posix()}"]
    assert msg == expected


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
def test_override_base_validation_msg_repo_variants(
    tmp_path, caplog, RepoClass, expect_warning, expected_version, warning_substr
):
    """Test clean, dirty, and no-version repository scenarios for override base validation message."""
    override_path = tmp_path / "override"
    override_path.mkdir()

    cv = ClusterVariables({}, MagicMock(spec=Collections))
    caplog.set_level(logging.WARNING)

    msg = cv._get_override_base_validation_msg(
        override_path, repository_class=RepoClass
    )

    # Base message always present
    assert msg[0] == f"Update variables from override: {override_path.as_posix()}"
    # Version line present
    assert msg[1] == f"Repository version: {expected_version}"

    if expect_warning:
        # Exactly one warning expected
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(warning_substr in str(w) for w in warnings)
    else:
        # No warnings for clean repo
        assert not any(r.levelno == logging.WARNING for r in caplog.records)


# Tests for ClusterVariables._get_collection_base_validation_msg


def test_collection_base_validation_msg_no_collection(tmp_path):
    """If read_galaxy_version returns None and get_repository returns None, only the
    base messages should appear.
    """
    default_dir = tmp_path / "collection_dir"
    default_dir.mkdir()

    collections = MagicMock(default_vars_dirs={}, _collection_readers=[])
    cv = ClusterVariables({}, collections)

    # Dummy collection reader with property on name
    coll = MagicMock(spec=CollectionReader)
    coll.name = "c1"
    coll.default_vars_directory = default_dir
    coll.read_galaxy_version.return_value = None
    coll.get_repository.return_value = None

    msg = cv._get_collection_base_validation_msg(coll)
    expected = [
        "Update variables from collection: c1",
        f"Path: {default_dir.as_posix()}",
    ]
    assert msg == expected


def test_collection_base_validation_msg_with_galaxy_version(tmp_path):
    """If read_galaxy_version returns a version string, it's included in the message."""
    default_dir = tmp_path / "collection_dir"
    default_dir.mkdir()

    collections = MagicMock(default_vars_dirs={}, _collection_readers=[])
    cv = ClusterVariables({}, collections)

    coll = MagicMock(spec=CollectionReader)
    coll.name = "c1"
    coll.default_vars_directory = default_dir
    coll.read_galaxy_version.return_value = "1.2.3"
    coll.get_repository.return_value = None

    msg = cv._get_collection_base_validation_msg(coll)
    expected = [
        "Update variables from collection: c1",
        f"Path: {default_dir.as_posix()}",
        "Galaxy collection version: 1.2.3",
    ]
    assert msg == expected


@pytest.mark.parametrize(
    "RepoClass, expect_warning, expected_version, warning_substr",
    [
        (FakeCleanRepo, False, "v1.0", None),
        (FakeDirtyRepo, True, "v2.0", "is a repository but is not clean"),
        (FakeNoVersionRepo, True, "No version yet", "has no version yet"),
    ],
)
def test_collection_base_validation_msg_repo_variants(
    tmp_path, caplog, RepoClass, expect_warning, expected_version, warning_substr
):
    """Test clean, dirty, and no-version repository scenarios without galaxy version for collection base validation message."""
    default_dir = tmp_path / "collection_dir"
    default_dir.mkdir()

    cv = ClusterVariables({}, MagicMock(spec=Collections))
    caplog.set_level(logging.WARNING)

    coll = MagicMock(spec=CollectionReader)
    coll.name = "c1"
    coll.default_vars_directory = default_dir
    coll.read_galaxy_version.return_value = None
    coll.get_repository.return_value = RepoClass(default_dir)

    msg = cv._get_collection_base_validation_msg(coll)

    # Base messages
    assert msg[0] == "Update variables from collection: c1"
    assert msg[1] == f"Path: {default_dir.as_posix()}"
    # Repository version line present
    assert msg[2] == f"Repository version: {expected_version}"

    if expect_warning:
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(warning_substr in str(w) for w in warnings)
    else:
        assert not any(r.levelno == logging.WARNING for r in caplog.records)


@pytest.mark.parametrize(
    "RepoClass, expect_warning, expected_version, warning_substr",
    [
        (FakeCleanRepo, False, "v1.0", None),
        (FakeDirtyRepo, True, "v2.0", "is a repository but is not clean"),
        (FakeNoVersionRepo, True, "No version yet", "has no version yet"),
    ],
)
def test_collection_base_validation_msg_galaxy_and_repo_variants(
    tmp_path, caplog, RepoClass, expect_warning, expected_version, warning_substr
):
    """Test scenarios where both galaxy version and repository info are present for collection base validation message."""
    default_dir = tmp_path / "collection_dir"
    default_dir.mkdir()

    cv = ClusterVariables({}, MagicMock(spec=Collections))
    caplog.set_level(logging.WARNING)

    coll = MagicMock(spec=CollectionReader)
    coll.name = "c1"
    coll.default_vars_directory = default_dir
    coll.read_galaxy_version.return_value = "2.0"
    coll.get_repository.return_value = RepoClass(default_dir)

    msg = cv._get_collection_base_validation_msg(coll)

    # Base messages + galaxy version + repo version
    assert msg[0] == "Update variables from collection: c1"
    assert msg[1] == f"Path: {default_dir.as_posix()}"
    assert msg[2] == "Galaxy collection version: 2.0"
    assert msg[3] == f"Repository version: {expected_version}"

    if expect_warning:
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any(warning_substr in str(w) for w in warnings)
    else:
        assert not any(r.levelno == logging.WARNING for r in caplog.records)
