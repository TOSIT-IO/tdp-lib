# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Optional, Union
from typing import Mapping as MappingType

from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import EmptyCommit, NoVersionYet
from tdp.core.service_component_host_name import ServiceComponentHostName
from tdp.core.service_component_name import ServiceComponentName
from tdp.core.variables.service_variables import ServiceVariables

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.models.component_version_log import ComponentVersionLog
    from tdp.core.repository.repository import Repository

logger = logging.getLogger("tdp").getChild("cluster_variables")


class ClusterVariables(MappingType[str, ServiceVariables]):
    """Mapping of service names with their ServiceVariables instance."""

    def __init__(self, service_variables_dict: dict[str, ServiceVariables]):
        """Initialize a ClusterVariables object.

        Args:
            service_variables_dict: Dictionary of service name to ServiceVariables instance.
        """
        self._service_variables_dict = service_variables_dict

    def __getitem__(self, key):
        return self._service_variables_dict.__getitem__(key)

    def __len__(self) -> int:
        return self._service_variables_dict.__len__()

    def __iter__(self):
        return self._service_variables_dict.__iter__()

    @staticmethod
    def initialize_cluster_variables(
        collections: Collections,
        tdp_vars: Union[str, os.PathLike],
        override_folders: Iterable[Optional[Union[str, os.PathLike]]] = None,
        repository_class: Repository = GitRepository,
        validate: bool = False,
    ) -> "ClusterVariables":
        """Get an instance of ClusterVariables, initialize services repositories if needed.

        Args:
            collections: instance of Collections.
            tdp_vars: Path to the tdp_vars directory.
            override_folders: list of path(s) of tdp vars overrides.
            repository_class: instance of the type of Repository used.
            validate: Whether or not to validate the services schemas.

        Returns:
            Mapping of service names with their ServiceVariables instance.
        """
        if override_folders is None:
            override_folders = []

        tdp_vars = Path(tdp_vars)

        cluster_variables = {}

        collections_and_overrides = [
            (collection_name, collection.default_vars_directory.iterdir())
            for collection_name, collection in collections.items()
        ]

        for i, override_folder in enumerate(override_folders):
            override_folder = Path(override_folder)
            collections_and_overrides.append(
                (f"overrides_path_{i}", override_folder.iterdir())
            )

        # If the service was already initialized, we do not touch it
        services_initialized_by_this_function = set()
        for collection_name, folder_iterator in collections_and_overrides:
            for path in folder_iterator:
                if not path.is_dir():
                    continue
                service = path.name
                service_tdp_vars = tdp_vars / service
                try:
                    service_tdp_vars.mkdir(parents=True)
                    logger.info(
                        f"{service_tdp_vars.absolute()} does not exist, created"
                    )
                except FileExistsError:
                    if not service_tdp_vars.is_dir():
                        raise ValueError(
                            f"{service_tdp_vars.absolute()} should be a directory"
                        )

                if service in cluster_variables:
                    service_variables = cluster_variables[service]
                else:
                    repo = repository_class.init(service_tdp_vars)
                    schemas = collections.get_service_schema(service)
                    service_variables = ServiceVariables(service, repo, schemas)
                    cluster_variables[service] = service_variables

                try:
                    logger.info(
                        f"{service_variables.name} is already initialized at {service_variables.version}"
                    )
                except NoVersionYet:
                    services_initialized_by_this_function.add(service)

                if service in services_initialized_by_this_function:
                    try:
                        service_variables.update_from_variables_folder(
                            "add variables from " + collection_name, path
                        )
                    except EmptyCommit:
                        logger.warning(
                            f"override file {service_tdp_vars.absolute()} will not cause any change, no commit has been made"
                        )

        cluster_variables = ClusterVariables(cluster_variables)
        if validate:
            cluster_variables._validate_services_schemas()

        return cluster_variables

    @staticmethod
    def get_cluster_variables(
        collections: Collections,
        tdp_vars: Union[str, os.PathLike],
        repository_class: Repository = GitRepository,
        validate: bool = False,
    ):
        """Get an instance of ClusterVariables.

        Args:
            collections: Instance of Collections.
            tdp_vars: Path to the tdp_vars directory.
            repository_class: Instance of the type of repositories used.
            validate: Whether or not to validate the services schemas.

        Returns:
            Mapping of service names with their ServiceVariables instance.
        """
        cluster_variables = {}

        tdp_vars = Path(tdp_vars)
        for path in tdp_vars.iterdir():
            if path.is_dir():
                repo = repository_class(tdp_vars / path.name)
                schemas = collections.get_service_schema(path.stem)
                cluster_variables[path.name] = ServiceVariables(
                    path.name, repo, schemas
                )

        cluster_variables = ClusterVariables(cluster_variables)

        if validate:
            cluster_variables._validate_services_schemas()

        return cluster_variables

    def _validate_services_schemas(self):
        """Validate all services schemas."""
        for service in self.values():
            service.validate()

    def get_modified_service_components(
        self,
        services_components_versions: Iterable[ComponentVersionLog],
    ) -> set[ServiceComponentHostName]:
        """Get the list of modified services and components.

        Args:
            services_components_versions: List of coherent deployed components and
                services versions.

        Returns:
            Modified service components names with host.

        Raises:
            RuntimeError: If a service is deployed but its repository is missing.
        """
        modified_service_components: set[ServiceComponentHostName] = set()
        for service_component_version in services_components_versions:
            service_name = service_component_version.service
            if service_name not in self.keys():
                raise RuntimeError(
                    f"Service '{service_name}' is deployed but its repository is "
                    + "missing."
                )
            component_name = service_component_version.component
            version = service_component_version.version
            host = service_component_version.host
            service_component_name = ServiceComponentName(service_name, component_name)
            service_component_host_name = ServiceComponentHostName(
                service_component_name, host
            )

            # Check if the service component is modified
            if self[service_name].is_service_component_modified_from_version(
                service_component_name, version
            ):
                debug_msg = (
                    "Service" if service_component_name.is_service else "Component"
                ) + f" '{service_component_name.full_name}' has been modified"
                if host:
                    debug_msg += f" for host {host}"
                debug_msg += "."
                logger.debug(debug_msg)
                modified_service_components.add(service_component_host_name)

        return modified_service_components
