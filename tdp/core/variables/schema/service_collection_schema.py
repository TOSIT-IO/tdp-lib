# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from pathlib import Path

from jsonschema import exceptions

from tdp.core.variables.schema.exceptions import (
    InvalidSchemaError,
    InvalidVariablesError,
    SchemaNotFoundError,
)
from tdp.core.variables.schema.types import Schema
from tdp.core.variables.schema.validator import VariablesValidator
from tdp.core.variables.variables import VariablesDict


class ServiceCollectionSchema:
    """Service schema for a single collection."""

    def __init__(self, schema: Schema) -> None:
        """Initialize a schema.

        Args:
            schema: The schema.

        Raises:
            InvalidSchemaError: If the schema is invalid.
        """
        try:
            VariablesValidator.check_schema(schema)
        except exceptions.SchemaError as e:
            raise InvalidSchemaError() from e
        self._schema = schema

    @staticmethod
    def from_path(path: Path) -> ServiceCollectionSchema:
        """Instanciate a ServiceCollectionSchema from a path.

        Args:
            path: Path to the schema.

        Returns:
            The schema.

        Raises:
            InvalidSchemaError: If the schema is invalid.
            SchemaNotFoundError: If the schema is not found.
        """
        try:
            with open(path, "r") as f:
                schema = json.load(f)
        except FileNotFoundError as e:
            raise SchemaNotFoundError(path) from e
        except json.JSONDecodeError as e:
            raise InvalidSchemaError() from e
        return ServiceCollectionSchema(schema)

    def validate(self, variables: VariablesDict) -> None:
        """Validate variables against the schema.

        Args:
            variables: Variables to validate.

        Raises:
            InvalidSchemaError: If the schema is invalid.
            InvalidVariablesError: If the variables are invalid against the schema.
        """
        try:
            VariablesValidator(self._schema).validate(variables)
        except exceptions.ValidationError as e:
            raise InvalidVariablesError(variables.name) from e
        except exceptions.SchemaError as e:
            raise InvalidSchemaError() from e
