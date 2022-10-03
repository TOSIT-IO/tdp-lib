# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import OrderedDict
from pathlib import Path

from tdp.core.collection import YML_EXTENSION
from tdp.core.operation import Operation
from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import NoVersionYet
from tdp.core.variables import Variables, merge_hash

logger = logging.getLogger("tdp").getChild("git_repository")

SERVICE_NAME_MAX_LENGTH = 15


class ServiceVariables:
    def __init__(self, service_name, repository, dag):
        if len(service_name) > SERVICE_NAME_MAX_LENGTH:
            raise ValueError(f"{service_name} is longer than {SERVICE_NAME_MAX_LENGTH}")
        self._name = service_name
        self._repo = repository
        self._dag = dag

    @property
    def name(self):
        return self._name

    @property
    def repository(self):
        return self._repo

    @property
    def dag(self):
        return self._dag

    @property
    def version(self):
        return self.repository.current_version()

    @property
    def clean(self):
        return self.repository.is_clean()

    @property
    def path(self):
        return self.repository.path

    def get_component_name(self, component):
        operations_filtered = list(
            filter(
                lambda operation: operation.component == component,
                self._dag.services_operations[self.name],
            )
        )
        if operations_filtered:
            operation = operations_filtered[0]
            return self.name + "_" + operation.component
        raise ValueError(f"Service {self.name} does not have a component {component}")

    def initialize_variables(self, override_folder=None):

        # dict with filename as key and a list of paths as value
        # a service can have multiple variable files present
        # will look through every collections
        default_var_paths = OrderedDict()
        for collection in self.dag.collections.values():
            default_vars = collection.get_service_default_vars(self.name)
            if not default_vars:
                continue
            for name, path in default_vars:
                default_var_paths.setdefault(name, []).append(path)

        # If there is an override folder, search for varfiles and append to variables paths
        if override_folder:
            service_override_folder = Path(override_folder) / self.name
            if service_override_folder.exists() and service_override_folder.is_dir():
                for override_path in service_override_folder.glob("*" + YML_EXTENSION):
                    default_var_paths.setdefault(override_path.name, []).append(
                        override_path
                    )

        # If service has no default vars, put a key with a none value
        if not default_var_paths:
            default_var_paths[self.name + YML_EXTENSION] = None

        with self.repository.validate(
            f"{self.name}: initial commit"
        ) as repostiory, repostiory.open_var_files(
            default_var_paths.keys()
        ) as configurations:
            # open_var_files returns an OrderedDict with filename as key, and Variables as value
            for configuration_file, configuration in configurations.items():
                default_variables_paths = default_var_paths[configuration_file]
                if default_variables_paths:
                    logger.info(
                        f"Initializing {self.name} with defaults from {', '.join(str(path) for path in default_variables_paths)}"
                    )
                    merge_result = {}
                    for default_variables_path in default_variables_paths:
                        with Variables(default_variables_path).open("r") as variables:
                            merge_result = merge_hash(merge_result, variables)

                    configuration.update(merge_result)
                # service has no default vars
                else:
                    logger.info(f"Initializing {self.name} without variables")
                    pass

    @staticmethod
    def initialize_service_managers(dag, services_directory, override_folder=None):
        """get a dict of service managers, initialize all services if needed

        Args:
            dag (Dag): operations' DAG
            services_directory (Union[str, Path]): path of the tdp vars
            override_folder (Optional[str | Path]): path of tdp vars overrides

        Returns:
            Dict[str, ServiceManager]: mapping of service with their manager
        """
        services_directory = Path(services_directory)
        service_managers = {}

        for service in dag.services:
            service_directory = services_directory / service

            try:
                service_directory.mkdir(parents=True)
                logger.info(f"{service_directory.absolute()} does not exist, created")
            except FileExistsError:
                if not service_directory.is_dir():
                    raise ValueError(
                        f"{service_directory.absolute()} should be a directory"
                    )

            repo = GitRepository.init(service_directory)
            service_manager = ServiceVariables(service, repo, dag)
            try:
                logger.info(
                    f"{service_manager.name} is already initialized at {service_manager.version}"
                )
            except NoVersionYet:
                service_manager.initialize_variables(override_folder)

            service_managers[service] = service_manager

        return service_managers

    @staticmethod
    def get_service_managers(dag, services_directory):
        """get a dict of service managers

        Args:
            dag (Dag): operations' DAG
            services_directory (PathLike): path of the tdp vars

        Returns:
            Dict[str, ServiceManager]: mapping of service with their manager
        """
        services_directory = Path(services_directory)

        service_managers = {}

        for service in dag.services:
            repo = GitRepository(services_directory / service)
            service_managers[service] = ServiceVariables(service, repo, dag)

        return service_managers

    def components_modified(self, version):
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
                service_operations = self.dag.services_operations[operation.service]
                components_modified.update(
                    (c for c in service_operations if c.action == "config")
                )
            else:
                components_modified.add(operation)
        return list(components_modified)
