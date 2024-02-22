# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Iterable
from typing import Optional

from tdp.core.variables.schema.exceptions import (
    InvalidSchemaError,
    InvalidVariablesError,
    SchemaValidationError,
)
from tdp.core.variables.schema.service_collection_schema import ServiceCollectionSchema
from tdp.core.variables.variables import VariablesDict


class ServiceSchema:
    """Service schema.

    Gather schemas from all collections that define a schema for a given service.
    """

    def __init__(
        self, schemas: Optional[Iterable[ServiceCollectionSchema]] = None
    ) -> None:
        """Initialize a service schema.

        Args:
            schema: The schema.
        """
        self._schemas = list(schemas) if schemas else []

    def add_schema(self, schema: ServiceCollectionSchema) -> None:
        """Add a schema to the service schema.

        Args:
            schema: The schema to add.
        """
        self._schemas.append(schema)

    def validate(self, variables: VariablesDict) -> None:
        """Validate variables against the schema.

        Args:
            variables: Variables to validate.

        Raises:
            SchemaValidationError: If the variables are invalid against the schema.
        """
        errors = []
        for schema in self._schemas:
            try:
                schema.validate(variables)
            except (InvalidVariablesError, InvalidSchemaError) as e:
                errors.append(e)
        if errors:
            raise SchemaValidationError(errors)
