# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Generator, Iterable
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tdp.core.constants import SERVICE_NAME_MAX_LENGTH, YML_EXTENSION
from tdp.core.types import PathLike
from tdp.core.variables.schema.exceptions import SchemaValidationError
from tdp.core.variables.variables import (
    Variables,
    VariablesDict,
)

if TYPE_CHECKING:
    from tdp.core.entities.hostable_entity_name import HostableEntityName
    from tdp.core.repository.repository import Repository
    from tdp.core.variables.schema.service_schema import ServiceSchema
    from tdp.core.variables.variables import _VariablesIOWrapper

logger = logging.getLogger(__name__)


class ServiceVariables:
    """Variables of a service."""

    def __init__(self, repository: Repository, schema: Optional[ServiceSchema]):
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
    def schema(self) -> Optional[ServiceSchema]:
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

    def update_from_dir(
        self, input_dir: PathLike, /, *, validation_message: str
    ) -> None:
        """Update the service variables from an input directory.

        Input variables are merged with the existing ones. If a variable file is not
        present in the repository, it is created. If a variable file is present in the
        repository but not in the input directory, it is not modified.

        Changes are persisted to the `tdp_vars` service repository using the given
        validation message.

        Args:
            message: Validation message to use for the repository.
            input_dir: Path to the directory containing the variables files to import.
        """
        input_file_paths = list(Path(input_dir).glob("*" + YML_EXTENSION))
        # Open corresponding files in the repository.
        files_to_open = (input_file_path.name for input_file_path in input_file_paths)
        with self.open_files(
            files_to_open, validation_message=validation_message, create_if_missing=True
        ) as files:
            # Merge the input files into the repository files.
            for input_file_path in input_file_paths:
                with Variables(input_file_path).open("r") as input_file:
                    files[input_file_path.name].merge(input_file)

    @contextmanager
    def open_files(
        self,
        file_names: Iterable[str],
        /,
        *,
        validation_message: str,
        create_if_missing: bool = False,
    ) -> Generator[OrderedDict[str, _VariablesIOWrapper], None, None]:
        """Open files in the service repository.

        Allow to open multiple files in the service repository at once in a context
        manager. Files can be modified in the context manager. Changes are persisted to
        the `tdp_vars` service repository using the given validation message.

        Args:
            validation_message: Validation message to use for the repository.
            file_names: Names of the files to manage.
            create_if_missing: Whether to create the file if it does not exist.

        Yields:
            A dictionary of opened files.
        """
        open_files = OrderedDict()
        # exit stack ensure that all file are closed before exiting the context manager
        with ExitStack() as stack:
            for file_name in file_names:
                open_files[file_name] = stack.enter_context(
                    Variables(
                        self.path / file_name, create_if_missing=create_if_missing
                    ).open()
                )
            yield open_files
        # commit the files
        with self.repository.validate(validation_message) as repo:
            repo.add_for_validation(open_files.keys())

    def is_entity_modified_from_version(
        self, entity: HostableEntityName, version: str
    ) -> bool:
        """Check if a component has been modified since the given version.

        A component is modified if the component variable file is modified
        or if the service variable file of this component is modified.

        Args:
            version: From what version to look. It is most likely the deployed version.
            entity: Name of the service or component to check.

        Returns:
            True if the component has been modified, False otherwise.
        """
        return self._repo.is_file_modified(
            commit=version, path=entity.name + YML_EXTENSION
        ) or self._repo.is_file_modified(
            commit=version, path=entity.service + YML_EXTENSION
        )

    def validate(self) -> None:
        """Validates the service variables against the schema.

        Raises:
            SchemaValidationError: If the schema is invalid.
        """
        service_variables = VariablesDict({})
        sorted_paths = sorted(self.path.glob("*" + YML_EXTENSION))
        errors = []
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
            # Validate the variables against the schema
            if not self.schema:
                continue
            try:
                self.schema.validate(test_variables)
            except SchemaValidationError as e:
                errors.append(e)
        # Raise errors if any
        if errors:
            raise SchemaValidationError(errors)

        logger.debug(f"Service {self.name} is valid")
