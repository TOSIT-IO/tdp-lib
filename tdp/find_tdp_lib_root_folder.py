# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Optional


def find_tdp_lib_root_folder(current_path: Optional[Path] = None):
    # If current_path is not provided, start from the directory of this file
    if current_path is None:
        current_path = Path(__file__).parent

    while True:
        # Check if 'pyproject.toml' exists in the current directory
        if (current_path / "alembic.ini").exists():
            return current_path.resolve()

        parent_path = current_path.parent
        # If the parent path is the same as the current path, we have reached the root directory
        if parent_path == current_path:
            return None

        current_path = parent_path


if __name__ == "__main__":
    print(find_tdp_lib_root_folder())
