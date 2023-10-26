# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import jsonschema
from jsonschema import exceptions

from tdp.core.collection import YML_EXTENSION
from tdp.core.operation import SERVICE_NAME_MAX_LENGTH
from tdp.core.repository.repository import NoVersionYet
from tdp.core.types import PathLike
from tdp.core.variables.variables import Variables, VariablesDict

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.dag import Dag
    from tdp.core.repository.repository import Repository
    from tdp.core.service_component_name import ServiceComponentName

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


class ServiceVariables:
    """Variables of a service."""

    def __init__(self, service_name: str, repository: Repository):
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

    @property
    def name(self) -> str:
        """Name of the service."""
        return self._name

    @property
    def repository(self) -> Repository:
        """Repository of the service."""
        return self._repo

    @property
    def version(self) -> Optional[str]:
        """Version of the service configuration."""
        try:
            return self.repository.current_version()
        except NoVersionYet:
            return None

    @property
    def clean(self) -> bool:
        """Whether the service repository is clean."""
        return self.repository.is_clean()

    @property
    def path(self) -> Path:
        """Path of the service repository."""
        return self.repository.path

    # TODO: rename to `get_component_variables`
    def get_variables(self, component_name: str) -> dict:
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
        self, message: str, tdp_vars_overrides: PathLike
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
                logger.info(f"updating '{self.name}' with variables from {file}")
                with Variables(file).open("r") as variables:
                    configurations[file.name].merge(variables)

    @contextmanager
    def _open_var_file(
        self, path: PathLike, fail_if_does_not_exist: bool = False
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
        self, message: str, paths: list[str], fail_if_does_not_exist: bool = False
    ) -> Generator[OrderedDict[str, Variables], None, None]:
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

    def is_sc_modified_from_version(
        self, service_component: ServiceComponentName, version: str
    ) -> bool:
        """Check if a component has been modified since the given version.

        A component is modified if the component variable file is modified
        or if the service variable file of this component is modified.

        Args:
            version: From what version to look. It is most likely the deployed version.
            service_component: Name of the service or component to check.

        Returns:
            True if the component has been modified, False otherwise.
        """
        return self._repo.is_file_modified(
            commit=version, path=service_component.full_name + YML_EXTENSION
        ) or self._repo.is_file_modified(
            commit=version, path=service_component.service_name + YML_EXTENSION
        )

    def validate_schema(self, variables: VariablesDict, schema: dict) -> None:
        """Validate variables against a schema.

        Args:
            variables: Variables to validate.
            schema: Schema to validate against.

        Raises:
            InvalidSchema: If the schema is invalid.
        """
        try:
            jsonschema.validate(dict(variables), schema, jsonschema.Draft7Validator)
        except exceptions.ValidationError as e:
            raise InvalidSchema("Schema is invalid", variables.name) from e

    def validate(self, collections: Collections) -> None:
        """Validates the service schema.

        Raises:
            InvalidSchema: If the schema is invalid.
        """
        schema = collections.get_service_schema(self.name)

        vars_paths = collections.get_default_vars_paths(self.name)
        vars_paths = sorted(vars_paths) if vars_paths else []
        vars_paths.extend(sorted(self.path.glob("*" + YML_EXTENSION)))

        service_variables = VariablesDict({}, self.name)
        for path in vars_paths:
            with Variables(path).open("r") as variables:
                service_variables.merge(variables.copy())
        self.validate_schema(service_variables, schema)
        logger.debug(f"'{self.name}' variables are valid")
