# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from tdp.core.collection import YML_EXTENSION
from tdp.core.operation import SERVICE_NAME_MAX_LENGTH
from tdp.core.types import PathLike
from tdp.core.variables.schema import validate_against_schema
from tdp.core.variables.variables import Variables, VariablesDict

if TYPE_CHECKING:
    from tdp.core.repository.repository import Repository
    from tdp.core.service_component_name import ServiceComponentName

logger = logging.getLogger(__name__)


class ServiceVariables:
    """Variables of a service."""

    def __init__(self, repository: Repository, schema: dict):
        """Initialize a ServiceVariables object.

        Args:
            service_name: Service name.
            repository: Repository of the service.
            schema: Schema for the service.

        Raises:
            ValueError: If the service name is longer than SERVICE_NAME_MAX_LENGTH.
        """
        self._repo = repository
        # Check that the service name is not too long
        if len(self.name) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(f"{self.name} is longer than {SERVICE_NAME_MAX_LENGTH}")
        self._schema = schema

    @property
    def name(self) -> str:
        """Name of the service."""
        return self.path.name

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
                logger.info(f"Updating {self.name} with variables from {file}")
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

    def validate(self) -> None:
        """Validates the service variables against the schema.

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
            validate_against_schema(test_variables, self.schema)
        logger.debug(f"Service {self.name} is valid")
