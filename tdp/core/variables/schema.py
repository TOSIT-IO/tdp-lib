# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import TYPE_CHECKING

import jsonschema
from jsonschema import exceptions

if TYPE_CHECKING:
    from tdp.core.variables.variables import VariablesDict

logger = logging.getLogger(__name__)


class InvalidSchema(Exception):
    """Schema is invalid."""

    def __init__(self, msg, filename, *args):
        self.msg = msg
        self.filename = filename
        super().__init__(msg, filename, *args)

    def __str__(self):
        return f"{self.msg}: {self.filename}"

    def __repr__(self):
        return f"{self.msg}: {self.filename}"


def validate_against_schema(variables: "VariablesDict", schema: dict) -> None:
    """Validate variables against a schema.

    Args:
        variables: Variables to validate.
        schema: Schema to validate against.

    Raises:
        InvalidSchema: If the schema is invalid.
    """
    try:
        jsonschema.validate(variables, schema, jsonschema.Draft7Validator)
        logger.debug(f"{variables.name} is valid against service schema")
    except exceptions.ValidationError as e:
        raise InvalidSchema("Schema is invalid", variables.name) from e
