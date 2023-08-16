# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum, EnumMeta


class _MetaEnum(EnumMeta):
    """Meta class for Enum."""

    def __contains__(cls: _MetaEnum, item: str) -> bool:
        """Check if value is a valid Enum value.

        Args:
            value: Value to check.

        Returns:
            True if value is a valid Enum value, False otherwise.
        """
        try:
            cls(item)
        except ValueError:
            return False
        return True


class BaseEnum(str, Enum, metaclass=_MetaEnum):
    """Base class for Enum."""

    pass
