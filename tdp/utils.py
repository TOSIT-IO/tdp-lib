# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import shutil
from typing import Optional, Sequence, TypeVar


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


T = TypeVar("T")


def get_previous_item(
    items: Sequence[T], current_item: T, default: Optional[T] = None
) -> Optional[T]:
    """Get the previous item in a sequence.

    Args:
        items: Sequence of items.
        current_item: Current item.
        default: Default value to return if the current item is the first of the
          sequence.

    Returns: The previous item in the sequence or the default value if the current item
      is the first item.
    """
    try:
        current_index = items.index(current_item)
        return items[current_index - 1] if current_index > 0 else default
    except ValueError:
        return default
