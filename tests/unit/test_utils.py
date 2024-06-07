# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import shutil

import pytest

from tdp.utils import ExecutableNotFoundError, resolve_executable


class TestResolveExecutable:
    def test_resolve_executable_as_path(self):
        # Test when as_path is True
        assert resolve_executable("python") == shutil.which("python")

    def test_resolve_executable_not_found(self):
        # Test when executable is not found in PATH
        with pytest.raises(ExecutableNotFoundError):
            resolve_executable("nonexistent_executable")
