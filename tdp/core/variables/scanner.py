# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

from tdp.core.constants import YML_EXTENSION


class ServiceDirectoryScanner:
    """Scans paths to identify valid service directories based on YML content.

    A service directory is considered valid if it contains at least one file with the
    YML extension.
    """

    @staticmethod
    def scan(path: Path) -> dict[str, Path]:
        return {
            p.name: p
            for p in path.iterdir()
            if p.is_dir() and any(p.glob("*" + YML_EXTENSION))
        }
