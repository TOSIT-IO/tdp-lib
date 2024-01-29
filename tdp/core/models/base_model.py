# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy.orm import DeclarativeBase

from tdp.core.utils import BaseEnum

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo


class BaseModel(DeclarativeBase):
    """Custom base class for SQLAlchemy models."""

    def to_dict(
        self,
        *,
        filter_out: Optional[list[str]] = None,
        format: Optional[bool] = True,
    ) -> dict[str, Any]:
        """Convert the model to a dictionary.

        Args:
            filter_out: List of columns to filter out.
            format: Whether to format the values for printing.

        Returns:
            Dictionary representation of the model.
        """
        filter_out = filter_out or []
        return {
            c.name: (
                self._formater(c.name, getattr(self, c.name))
                if format
                else getattr(self, c.name)
            )
            for c in self.__table__.columns
            if c.name not in filter_out
        }

    def __repr__(self):
        """Return a string representation of the model."""
        values = ", ".join(f"{key}={value}" for key, value in self.to_dict().items())
        return f"{self.__class__.__name__}({values})"

    def _formater(self, key: str, value: Optional[Any]) -> str:
        """Format a value for printing."""
        if isinstance(value, dict):
            return str(
                {key: self._formater(key, value) for key, value in value.items()}
            )
        elif isinstance(value, list):
            if len(value) > 2:
                return f"[{value[0]}, ..., {value[-1]}]"
            return str(value)
        elif isinstance(value, BaseEnum):
            return value.name
        elif isinstance(value, datetime):
            return (
                value.replace(tzinfo=timezone.utc)
                .astimezone(LOCAL_TIMEZONE)
                .strftime("%Y-%m-%d %H:%M:%S")
            )
        elif value is None:
            return ""
        return str(value)
