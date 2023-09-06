# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Tuple

from sqlalchemy.orm import DeclarativeBase


def keyvalgen(obj: object) -> Tuple[str, Any]:
    # From: https://stackoverflow.com/a/54034230
    """Generate key-value pairs from an object.

    Exclude keys starting with an underscore and keys that are SQLAlchemy.

    Args:
        obj: Object to generate key-value pairs from.

    Yields:
        tuple of key-value pair.
    """
    excl = ("_sa_adapter", "_sa_instance_state")
    for k, v in vars(obj).items():
        if not k.startswith("_") and not any(hasattr(v, a) for a in excl):
            yield k, v


class Base(DeclarativeBase):
    """Custom base class for SQLAlchemy models."""

    def __repr__(self):
        params = ", ".join(f"{k}={v}" for k, v in keyvalgen(self))
        return f"{self.__class__.__name__}({params})"
