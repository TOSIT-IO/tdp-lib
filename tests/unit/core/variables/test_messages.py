# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import pathlib
from unittest.mock import MagicMock

import pytest

from tdp.core.constants import VALIDATION_MESSAGE_FILE
from tdp.core.repository.repository import NoVersionYet
from tdp.core.variables.messages import ValidationMessageBuilder


class TestValidationMessageBuilderForService:
    def test_returns_custom_message(self, tmp_path):
        service_dir = tmp_path / "service"
        service_dir.mkdir()
        msg_file = service_dir / VALIDATION_MESSAGE_FILE
        custom_msg = "Custom validation message"
        msg_file.write_text(custom_msg)

        builder = ValidationMessageBuilder(collections=MagicMock())
        result = builder.for_service(service_dir)
        assert result == custom_msg

    def test_returns_none_when_file_missing(self, tmp_path):
        service_dir = tmp_path / "service"
        service_dir.mkdir()

        builder = ValidationMessageBuilder(collections=MagicMock())
        result = builder.for_service(service_dir)
        assert result is None

    def test_supports_custom_filename(self, tmp_path):
        service_dir = tmp_path / "service"
        service_dir.mkdir()
        custom_file = service_dir / "MY_MSG"
        custom_msg = "Another message"
        custom_file.write_text(custom_msg)

        builder = ValidationMessageBuilder(
            collections=MagicMock(), validation_msg_file_name="MY_MSG"
        )
        result = builder.for_service(service_dir)
        assert result == custom_msg

    def test_strips_whitespace(self, tmp_path):
        service_dir = tmp_path / "service"
        service_dir.mkdir()
        msg_file = service_dir / VALIDATION_MESSAGE_FILE
        msg_file.write_text("  message with spaces  \n")

        builder = ValidationMessageBuilder(collections=MagicMock())
        result = builder.for_service(service_dir)
        assert result == "message with spaces"

    def test_file_not_readable(self, tmp_path):
        service_dir = tmp_path / "service"
        service_dir.mkdir()
        msg_file = service_dir / VALIDATION_MESSAGE_FILE
        msg_file.write_text("Should not be readable")
        msg_file.chmod(0o000)

        builder = ValidationMessageBuilder(collections=MagicMock())
        try:
            result = builder.for_service(service_dir)
            assert result is None
        except Exception as e:
            assert isinstance(e, IOError)
        finally:
            msg_file.chmod(0o644)

    def test_returns_none_if_path_is_file(self, tmp_path):
        service_file = tmp_path / "service"
        service_file.write_text("not a directory")

        builder = ValidationMessageBuilder(collections=MagicMock())
        result = builder.for_service(service_file)
        assert result is None

    def test_symlink_file(self, tmp_path):
        service_dir = tmp_path / "service"
        service_dir.mkdir()
        real_file = service_dir / "real_msg"
        real_file.write_text("Symlinked message")
        symlink_file = service_dir / VALIDATION_MESSAGE_FILE
        symlink_file.symlink_to(real_file)

        builder = ValidationMessageBuilder(collections=MagicMock())
        result = builder.for_service(service_dir)
        assert result == "Symlinked message"


class TestValidationMessageBuilderForOverride:
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
    def test_override_variants(
        self,
        tmp_path,
        caplog,
        RepoClass,
        expect_warning,
        expected_version,
        warning_substr,
    ):
        override_path = tmp_path / "override"
        override_path.mkdir()

        builder = ValidationMessageBuilder(
            collections=MagicMock(), repository_class=RepoClass
        )
        msg = builder.for_override(override_path)

        assert f"Update variables from override: {override_path.as_posix()}" in msg
        assert f"Repository version: {expected_version}" in msg

        if expect_warning:
            warnings = [
                r.message for r in caplog.records if r.levelno == logging.WARNING
            ]
            assert any(warning_substr in str(w) for w in warnings)
        else:
            assert not any(r.levelno == logging.WARNING for r in caplog.records)


class TestValidationMessageBuilderForCollection:
    def test_includes_collection_path_and_versions(self):
        mock_collections = MagicMock()
        mock_collections.default_vars_dirs = {"hive": pathlib.Path("/fake/path")}
        mock_collections.get_version.return_value = MagicMock(
            galaxy="1.2.3", repo="v4.5.6"
        )

        builder = ValidationMessageBuilder(collections=mock_collections)
        result = builder.for_collection("hive")

        assert "Update variables from collection: hive" in result
        assert "Path: /fake/path" in result
        assert "Galaxy collection version: 1.2.3" in result
        assert "Repository version: v4.5.6" in result
