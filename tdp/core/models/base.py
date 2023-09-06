# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Any

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Custom base class for SQLAlchemy models."""

    def to_dict(self) -> dict[str, Any]:
        """Convert the model to a dictionary.

        Returns:
            Dictionary representation of the model.
        """
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def __repr__(self):
        """Return a string representation of the model."""
        values = ", ".join(f"{key}={value}" for key, value in self.to_dict().items())
        return f"{self.__class__.__name__}({values})"
