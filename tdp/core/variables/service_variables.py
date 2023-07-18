# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
import os
from collections import OrderedDict
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Dict, Iterator, List, Set, Union

import jsonschema
from jsonschema import exceptions, validators

from tdp.core.collection import YML_EXTENSION
from tdp.core.service_component_name import ServiceComponentName
from tdp.core.dag import Dag
from tdp.core.operation import SERVICE_NAME_MAX_LENGTH
from tdp.core.repository.repository import Repository
from tdp.core.models import ComponentVersionLog

from .variables import Variables, VariablesDict, is_object

logger = logging.getLogger("tdp").getChild("service_variables")


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


type_checker = jsonschema.Draft7Validator.TYPE_CHECKER.redefine("object", is_object)

CustomValidator = validators.extend(
    jsonschema.Draft7Validator, type_checker=type_checker
)


class ServiceVariables:
    """Variables of a service.

    Attributes:
        name: Service name.
        repository: Repository of the service.
        schema: Schema for the service.
    """

    def __init__(self, service_name: str, repository: Repository, schema: dict):
        """Initialize a ServiceVariables object.

        Args:
            service_name: Service name.
            repository: Repository of the service.
            schema: Schema for the service.

        Raises:
            ValueError: If the service name is longer than SERVICE_NAME_MAX_LENGTH.
        """
        if len(service_name) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(f"{service_name} is longer than {SERVICE_NAME_MAX_LENGTH}")
        self._name = service_name
        self._repo = repository
        self._schema = schema

    @property
    def name(self) -> str:
        """Name of the service."""
        return self._name

    @property
    def repository(self) -> Repository:
        """Repository of the service."""
        return self._repo

    @property
    def schema(self) -> dict:
        """Schema of the service."""
        return self._schema

    @property
    def version(self) -> str:
        """Version of the service configuration."""
        return self.repository.current_version()

    @property
    def clean(self) -> bool:
        """Whether the service repository is clean."""
        return self.repository.is_clean()

    @property
    def path(self) -> Path:
        """Path of the service repository."""
        return self.repository.path

    # TODO: rename to `get_component_variables`
    def get_variables(self, component_name: str) -> Dict:
        """Get the variables for a component.

        Args:
            component_name: Name of the component.

        Returns:
            Copy of the variables of the component.
        """
        component_path = self._repo.path / (component_name + YML_EXTENSION)
        if not component_path.exists():
            return None
        with Variables(component_path).open("r") as variables:
            return variables.copy()

    # TODO: move this function outside of this class, or move the dag part
    def get_component_name(self, dag: Dag, component_name: str) -> str:
        """Get the full component name.

        Args:
            dag: Dag instance.
            component_name: Name of the component.

        Returns:
            <service_name>_<component_name>

        Raises:
            ValueError: If component does not exist.
        """
        operations_filtered = list(
            filter(
                lambda operation: operation.component == component_name,
                dag.services_operations[self.name],
            )
        )
        if operations_filtered:
            operation = operations_filtered[0]
            return self.name + "_" + operation.component
        raise ValueError(
            f"Service {self.name} does not have a component {component_name}"
        )

    def update_from_variables_folder(
        self, message: str, tdp_vars_overrides: Union[str, os.PathLike]
    ) -> None:
        """Update the variables repository from an overrides file.

        Args:
            message: Validation message.
            tdp_vars_overrides: Overrides file path.
        """
        override_files = list(Path(tdp_vars_overrides).glob("*" + YML_EXTENSION))
        service_files_to_open = [override_file.name for override_file in override_files]
        with self.open_var_files(f"{message}", service_files_to_open) as configurations:
            for file in override_files:
                logger.info(f"Updating {self.name} with variables from {file}")
                with Variables(file).open("r") as variables:
                    configurations[file.name].merge(variables)

    @contextmanager
    def _open_var_file(
        self, path: Union[str, os.PathLike], fail_if_does_not_exist: bool = False
    ) -> Variables:
        """Context manager to facilitate the opening a variables file.

        Provides a Variables object automatically closed when parent context manager closes it.

        Args:
            path: Path of the variables file to open.
            fail_if_does_not_exist: Whether or not the function should raise an error when file does not exist.

        Yields:
            A weakref of the Variables object, to prevent the creation of strong references
                outside the caller's context.

        Raises:
            ValueError: If the file does not exist and fail_if_does_not_exist is True.
        """
        path = self.path / path
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            if fail_if_does_not_exist:
                raise ValueError("Path does not exist")
            path.touch()
        with Variables(path).open() as variables:
            yield variables

    @contextmanager
    def open_var_files(
        self, message: str, paths: List[str], fail_if_does_not_exist: bool = False
    ) -> Iterator[
        Dict[str, Variables]
    ]:  # TODO: Transform Dict to OrderedDict with python>3.6
        """Open variables files.

        Adds the underlying files for validation.

        Args:
            message: Validation message.
            paths: List of paths to open.

        Yields:
            Variables as an OrderedDict where keys are sorted by the order of the input paths.
        """
        with self.repository.validate(message), ExitStack() as stack:
            yield OrderedDict(
                (
                    path,
                    stack.enter_context(
                        self._open_var_file(path, fail_if_does_not_exist)
                    ),
                )
                for path in paths
            )
            stack.close()
            self.repository.add_for_validation(paths)

    def is_service_component_modified_from_version(
        self, service_component: ServiceComponentName, version: str
    ) -> bool:
        """Check if a component has been modified since the given version.

        Args:
            version: From what version to look. It is most likely the deployed version.
            service_component: Name of the service or component to check.

        Returns:
            True if the component has been modified, False otherwise.
        """
        return self._repo.is_file_modified(
            commit=version,
            file=service_component.full_name + YML_EXTENSION,
        )

    def validate_schema(self, variables: Variables, schema: dict) -> None:
        """Validate variables against a schema.

        Args:
            variables: Variables to validate.
            schema: Schema to validate against.

        Raises:
            InvalidSchema: If the schema is invalid.
        """
        try:
            jsonschema.validate(variables, schema, CustomValidator)
        except exceptions.ValidationError as e:
            raise InvalidSchema("Schema is invalid", variables.name) from e

    def validate(self) -> None:
        """Validates the service schema.

        Raises:
            InvalidSchema: If the schema is invalid.
        """
        service_variables = VariablesDict({})
        sorted_paths = sorted(self.path.glob("*" + YML_EXTENSION))
        for path in sorted_paths:
            with Variables(path).open("r") as variables:
                if path.stem == self.name:
                    # create a copy of the dict (not to outlive the context manager)
                    service_variables = VariablesDict(variables.copy(), variables.name)
                    test_variables = service_variables
                else:
                    # merge on a copy of service_vars because merge is inplace
                    test_variables = VariablesDict(
                        service_variables.copy(), variables.name
                    )
                    test_variables.merge(variables)
            self.validate_schema(test_variables, self.schema)
            logger.debug(f"{path.stem} is valid")
        logger.debug(f"Service {self.name} is valid")
