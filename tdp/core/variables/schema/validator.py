# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from jsonschema import Draft202012Validator, TypeChecker, validators

from tdp.core.variables.variables import VariablesDict


def is_object(checker: TypeChecker, instance: object) -> bool:
    """Return a function that checks if a value is an object."""
    return Draft202012Validator.TYPE_CHECKER.is_type(instance, "object") or isinstance(
        instance, VariablesDict
    )


# Custom validator that allows VariablesDict to be validated as objects
VariablesValidator = validators.extend(
    Draft202012Validator,
    type_checker=Draft202012Validator.TYPE_CHECKER.redefine("object", is_object),
)
