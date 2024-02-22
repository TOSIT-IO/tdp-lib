# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from typing import Optional


class InvalidSchemaError(Exception):
    """Raised when a schema is invalid.

    Attributes:
        schema_path: The path to the schema.
    """

    def __init__(self, schema_path: Optional[Path] = None) -> None:
        super().__init__("Invalid schema", schema_path)
        self.schema_path = Path(schema_path) if schema_path else None

    def __str__(self) -> str:
        if not self.schema_path:
            return self.args[0]
        return f"{self.args[0]} at {self.schema_path}"

    def __repr__(self) -> str:
        return (
            f"InvalidSchemaError(msg={self.args[0]}, schema_path={self.schema_path!r})"
        )


class InvalidVariablesError(Exception):
    """Variables are invalid."""

    def __init__(self, filename):
        self.filename = filename
        super().__init__("Validation against schema failed", filename)

    def __str__(self):
        return f"{self.args[0]} for {self.filename}"

    def __repr__(self):
        return f"InvalidVariablesError(msg={self.args[0]}, filename={self.filename}))"


class SchemaNotFoundError(Exception):
    """Raised when a schema is not found.

    Attributes:
        schema_path: The path to the schema.
    """

    def __init__(self, schema_path: Path) -> None:
        super().__init__("Schema not found", schema_path)
        self.schema_path = Path(schema_path)

    def __str__(self) -> str:
        return f"{self.args[0]} at {self.schema_path}"

    def __repr__(self) -> str:
        return (
            f"MissingSchemaError(msg={self.args[0]}, schema_path={self.schema_path!r})"
        )


class SchemaValidationError(Exception):
    """Validation failed."""

    def __init__(self, errors: list[Exception]):
        """Initialize a ValidationFailedError object.

        Args:
            errors: List of errors.
        """
        self.errors = errors

    def __str__(self):
        return "\n".join(str(error) for error in self.errors)

    def __repr__(self):
        return "\n".join(repr(error) for error in self.errors)
