# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import EmptyCommit
from tdp.core.types import PathLike
from tdp.core.variables.service_variables import ServiceVariables

if TYPE_CHECKING:
    from tdp.core.cluster_status import SCHStatus
    from tdp.core.collections import Collections
    from tdp.core.repository.repository import Repository
    from tdp.core.service_component_host_name import ServiceComponentHostName

logger = logging.getLogger(__name__)


class ClusterVariables(Mapping[str, ServiceVariables]):
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
    def initialize_tdp_vars(
        collections: Collections,
        tdp_vars_path: PathLike,
        input_vars: Optional[Iterable[PathLike]] = None,
        repository_class: type[Repository] = GitRepository,
        validate: bool = False,
    ) -> "ClusterVariables":
        """Intialize the tdp_vars directory.

        Create a ServiceVariables instance for each service in the collections.
        Initialize the repository of each service with the content given as input_vars.

        Args:
            collections: Collections instance.
            tdp_vars: Path to the tdp_vars directory.
            input_vars: Variables folders to be added to the repositories.
            repository_class: Type of Repository to use to store the variables.
            validate: Whether or not to validate the variables against the schema.

        Returns:
            ClusterVariables instance.
        """
        tdp_vars_path = Path(tdp_vars_path)
        cluster_variables = {}

        # TODO: Construct the cluster variables from the var defaults
        for service_name in collections.get_services():
            tdp_vars_service = tdp_vars_path / service_name

            service_variables = ServiceVariables(
                service_name=service_name,
                repository=repository_class.init(tdp_vars_service),
            )
            cluster_variables[service_name] = service_variables

            # Add variables from input vars to the repository
            for vars_folder in input_vars or []:
                # Skip if the folder does not exist
                if not Path(vars_folder).is_dir():
                    continue
                for service_folder in Path(vars_folder).iterdir():
                    if not service_folder.is_dir():
                        continue
                    if service_folder.name == service_name:
                        try:
                            service_variables.update_from_variables_folder(
                                "add variables from " + str(vars_folder), service_folder
                            )
                        except EmptyCommit:
                            logger.warning(
                                f"override file {tdp_vars_service.absolute()} will"
                                " not cause any change, no commit has been made"
                            )
                        break

            if validate:
                service_variables.validate(collections)

        return ClusterVariables(cluster_variables)

    @staticmethod
    def get_cluster_variables(
        collections: Collections,
        tdp_vars_path: PathLike,
        # TODO: repository_class should be recovered from the repository itself
        repository_class: type[Repository] = GitRepository,
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

        # Instanciate a ServiceVariable object for each service defined in the tdp_vars
        # directory and register it in the cluster_variables dictionary.
        tdp_vars_path = Path(tdp_vars_path)
        for path in tdp_vars_path.iterdir():
            if path.is_dir():
                repo = repository_class(tdp_vars_path / path.name)
                cluster_variables[path.name] = ServiceVariables(path.name, repo)

        cluster_variables = ClusterVariables(cluster_variables)

        # Validate the variable against the schema if needed.
        if validate:
            cluster_variables._validate_services_schemas(collections)

        return cluster_variables

    def _validate_services_schemas(self, collections: Collections):
        """Validate all services schemas."""
        for service in self.values():
            service.validate(collections)

    def get_modified_sch(
        self,
        sch_statuses: Iterable[SCHStatus],
    ) -> set[ServiceComponentHostName]:
        """Get the list of modified sch.

        Args:
            sch_statuses: List of deployed sch statuses.

        Returns:
            Modified service components names with host.

        Raises:
            RuntimeError: If a service is deployed but its repository is missing.
        """
        modified_sch: set[ServiceComponentHostName] = set()
        for status in sch_statuses:
            # Check if the service exist
            if status.service not in self.keys():
                raise RuntimeError(
                    f"Service '{status.service}' is deployed but its repository is "
                    + "missing."
                )

            sch = status.get_sch_name()
            sc = sch.service_component_name

            # Skip if no newer version is available
            if status.configured_version and not self[
                sc.service_name
            ].is_sc_modified_from_version(sc, status.configured_version):
                continue

            logger.debug(
                f"{sc.full_name} has been modified"
                + (f" for host {sch.host_name}" if sch.host_name else "")
            )

            modified_sch.add(sch)

        return modified_sch
