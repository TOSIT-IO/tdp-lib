# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Mapping
from pathlib import Path

from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import NoVersionYet
from tdp.core.variables.service_variables import ServiceVariables

logger = logging.getLogger("tdp").getChild("cluster_variables")


class ClusterVariables(Mapping):
    def __init__(self, service_variables_dict):
        self._service_variables_dict = service_variables_dict

    def __getitem__(self, key):
        return self._service_variables_dict.__getitem__(key)

    def __len__(self) -> int:
        return self._service_variables_dict.__len__()

    def __iter__(self):
        return self._service_variables_dict.__iter__()

    @staticmethod
    def initialize_cluster_variables(collections, tdp_vars, override_folder=None):
        """get an instance of ClusterVariables, initialize all services if needed

        Args:
            collections (Collections): instance of collections
            tdp_vars (Union[str, Path]): path to the tdp vars
            override_folder (Optional[str | Path]): path of tdp vars overrides

        Returns:
            ClusterVariables: mapping of service with their ServiceVariables instance
        """
        tdp_vars = Path(tdp_vars)

        cluster_variables = {}

        collections_and_overrides = [
            (collection_name, collection.default_vars_directory.iterdir())
            for collection_name, collection in collections.items()
        ]
        if override_folder:
            override_folder = Path(override_folder)
            collections_and_overrides.append(("overrides", override_folder.iterdir()))

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
                    repo = GitRepository.init(service_tdp_vars)
                    service_variables = ServiceVariables(service, repo)
                    cluster_variables[service] = service_variables

                try:
                    logger.info(
                        f"{service_variables.name} is already initialized at {service_variables.version}"
                    )
                except NoVersionYet:
                    services_initialized_by_this_function.add(service)

                if service in services_initialized_by_this_function:
                    service_variables.update_from_variables_folder(
                        "add variables from " + collection_name, path
                    )

        return ClusterVariables(cluster_variables)

    @staticmethod
    def get_cluster_variables(tdp_vars):
        """get an instance of ClusterVariables

        Args:
            tdp_vars (PathLike): path of the tdp vars

        Returns:
            ClusterVariables: mapping of service with their ServiceVariables instance
        """
        tdp_vars = Path(tdp_vars)

        cluster_variables = {}

        tdp_vars = Path(tdp_vars)
        for path in tdp_vars.iterdir():
            if path.is_dir():
                repo = GitRepository(tdp_vars / path.name)
                cluster_variables[path.name] = ServiceVariables(path.name, repo)

        return ClusterVariables(cluster_variables)
