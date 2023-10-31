# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import shutil


class ExecutableNotFoundError(Exception):
    """Raised when an executable is not found in PATH."""

    pass


def resolve_executable(executable: str, /) -> str:
    """Resolve an executable to its full path.

    Args:
        executable: Name of the executable to resolve.

    Returns:
        Path of the executable.

    Raises:
        ExecutableNotFoundError: If the executable is not found in PATH and as_path
          is True.
    """
    executable_path = shutil.which(executable)
    if executable_path is None:
        raise ExecutableNotFoundError(f"'{executable}' not found in PATH")
    return executable_path
