# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import OrderedDict
from contextlib import ExitStack, contextmanager
from pathlib import Path

from tdp.core.collection import YML_EXTENSION
from tdp.core.operation import Operation
from tdp.core.variables.variables import Variables

logger = logging.getLogger("tdp").getChild("service_variables")

SERVICE_NAME_MAX_LENGTH = 15


class ServiceVariables:
    def __init__(self, service_name, repository):
        if len(service_name) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(f"{service_name} is longer than {SERVICE_NAME_MAX_LENGTH}")
        self._name = service_name
        self._repo = repository

    @property
    def name(self):
        return self._name

    @property
    def repository(self):
        return self._repo

    @property
    def version(self):
        return self.repository.current_version()

    @property
    def clean(self):
        return self.repository.is_clean()

    @property
    def path(self):
        return self.repository.path

    def get_variables(self, component):
        """Return copy of the variables"""
        component_path = self._repo.path / (component + YML_EXTENSION)
        if not component_path.exists():
            return None
        with Variables(component_path).open("r") as variables:
            return variables.copy()

    # TODO: move this function outside of this class, or move the dag part
    def get_component_name(self, dag, component):
        operations_filtered = list(
            filter(
                lambda operation: operation.component == component,
                dag.services_operations[self.name],
            )
        )
        if operations_filtered:
            operation = operations_filtered[0]
            return self.name + "_" + operation.component
        raise ValueError(f"Service {self.name} does not have a component {component}")

    def update_from_variables_folder(self, message, tdp_vars_overrides):
        override_files = list(tdp_vars_overrides.glob("*" + YML_EXTENSION))
        service_files_to_open = [override_file.name for override_file in override_files]
        with self.open_var_files(f"{message}", service_files_to_open) as configurations:
            for file in override_files:
                logger.info(f"Updating {self.name} with variables from {file}")
                with Variables(file).open("r") as variables:
                    configurations[file.name].merge(variables)

    @contextmanager
    def _open_var_file(self, path, fail_if_does_not_exist=False):
        """Returns a Variables object managed, simplyfing use.

        Returns a Variables object automatically closed when parent context manager closes it.
        Args:
            path ([PathLike]): Path to open as a Variables file.
            fail_if_does_not_exist ([bool]): Whether or not the function should raise an error when file does not exist
        Yields:
            [Proxy[Variables]]: A weakref of the Variables object, to prevent the creation of strong references
                outside the caller's context
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
    def open_var_files(self, message, paths, fail_if_does_not_exist=False):
        """Returns an OrderedDict of dict[path] = Variables object

        Adds the underlying files for validation.
        Args:
            paths ([List[PathLike]]): List of paths to open

        Yields:
            [OrderedDict[PathLike, Variables]]: Returns an OrderedDict where keys
                are sorted by the order of the input paths
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

    # TODO: move this function outside of this class, or move the dag part
    def components_modified(self, dag, version):
        """get a list of operations that modified components since version

        Args:
            version (str): how far to look

        Returns:
            List[Operation]: operations that modified components
        """
        files_modified = self._repo.files_modified(version)
        components_modified = set()
        for file_modified in files_modified:
            operation = Operation(Path(file_modified).stem + "_config")
            # If operation is about a service, all components inside this service have to be returned
            if operation.is_service():
                service_operations = dag.services_operations[operation.service]
                components_modified.update(
                    (c for c in service_operations if c.action == "config")
                )
            else:
                components_modified.add(operation)
        return list(components_modified)
