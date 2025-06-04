# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tdp.core.collections.collection_reader import CollectionReader
from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import EmptyCommit, NoVersionYet
from tdp.core.types import PathLike
from tdp.core.variables.schema.exceptions import SchemaValidationError
from tdp.core.variables.service_variables import ServiceVariables

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.entities.hosted_entity import HostedEntity
    from tdp.core.entities.hosted_entity_status import HostedEntityStatus
    from tdp.core.repository.repository import Repository

logger = logging.getLogger(__name__)

DEFAULT_VALIDATION_MESSAGE = "Updated from one or more directories"
VALIDATION_MESSAGE_FILE = "COMMIT_EDITMSG"


class ServiceToUpdate:
    """Object representing a service to update.

    It is used to aggregate input paths and validation messages for a service before
    updating it.
    """

    def __init__(
        self,
        service_name: str,
        *,
        base_validation_message: str = DEFAULT_VALIDATION_MESSAGE,
    ):
        """Initialize a ServiceToUpdate object.

        Args:
            service_name: Name of the service to update.
            validation_message: Custom validation message for the service.
            input_paths: Paths to the input files for the service.
        """
        self.service_name = service_name
        self._validation_message = [base_validation_message]
        self.input_paths = []

    @property
    def validation_message(self) -> str:
        """Get the validation message for the service.

        Returns:
            The validation message as a string.
        """
        return "\n".join(self._validation_message)

    def add_input_path(
        self,
        input_path: PathLike,
        validation_msg: Optional[str] = None,
    ):
        """Add an input path for the service.

        Args:
            input_path: Path to the input file.
            validation_msg: Custom validation message for this input path.
        """
        self.input_paths.append(input_path)
        if validation_msg:
            self._validation_message.append(validation_msg)


class ClusterVariables(Mapping[str, ServiceVariables]):
    """Mapping of service names with their ServiceVariables instance."""

    def __init__(
        self,
        service_variables_dict: dict[str, ServiceVariables],
        collections: Collections,
    ):
        """Initialize a ClusterVariables object.

        Args:
            service_variables_dict: Dictionary of service name to ServiceVariables instance.
        """
        self._service_variables_dict = service_variables_dict
        self._collections = collections

    def __getitem__(self, key):
        return self._service_variables_dict.__getitem__(key)

    def __len__(self) -> int:
        return self._service_variables_dict.__len__()

    def __iter__(self):
        return self._service_variables_dict.__iter__()

    @staticmethod
    def initialize_cluster_variables(
        collections: Collections,
        tdp_vars: PathLike,
        override_folders: Optional[Iterable[PathLike]] = None,
        repository_class: type[Repository] = GitRepository,
        validate: bool = False,
    ) -> ClusterVariables:
        """Get an instance of ClusterVariables, initialize services repositories if needed.

        Args:
            collections: instance of Collections.
            tdp_vars: Path to the tdp_vars directory.
            override_folders: list of path(s) of tdp vars overrides.
            repository_class: instance of the type of Repository used.
            validate: Whether or not to validate the services schemas.

        Returns:
            Mapping of service names with their ServiceVariables instance.

        Raises:
            SchemaValidationError: If a service is invalid.
        """
        if override_folders is None:
            override_folders = []

        tdp_vars = Path(tdp_vars)

        cluster_variables = {}

        collections_and_overrides = [
            (collection_name, default_var_dir.iterdir())
            for [
                collection_name,
                default_var_dir,
            ] in collections.default_vars_dirs.items()
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
                    schemas = collections.schemas.get(service)
                    service_variables = ServiceVariables(repo, schemas)
                    cluster_variables[service] = service_variables

                try:
                    logger.info(
                        f"{service_variables.name} is already initialized at {service_variables.version}"
                    )
                except NoVersionYet:
                    services_initialized_by_this_function.add(service)

                if service in services_initialized_by_this_function:
                    try:
                        service_variables.update_from_dir(
                            path,
                            validation_message="add variables from " + collection_name,
                        )
                    except EmptyCommit:
                        logger.warning(
                            f"override file {service_tdp_vars.absolute()} will not cause any change, no commit has been made"
                        )

        cluster_variables = ClusterVariables(cluster_variables, collections=collections)
        if validate:
            cluster_variables._validate_services_schemas()

        return cluster_variables

    @staticmethod
    def get_cluster_variables(
        collections: Collections,
        tdp_vars: PathLike,
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

        Raises:
            SchemaValidationError: If a service is invalid.
        """
        cluster_variables = {}

        tdp_vars = Path(tdp_vars)
        for path in tdp_vars.iterdir():
            if path.is_dir():
                repo = repository_class(tdp_vars / path.name)
                schemas = collections.schemas.get(path.stem)
                cluster_variables[path.name] = ServiceVariables(repo, schemas)

        cluster_variables = ClusterVariables(cluster_variables, collections=collections)

        if validate:
            cluster_variables._validate_services_schemas()

        return cluster_variables

    def update(
        self,
        override_folders: Optional[Iterable[PathLike]] = None,
        validate=False,
        *,
        validation_msg_file_name: str = VALIDATION_MESSAGE_FILE,
        base_validation_msg: str = DEFAULT_VALIDATION_MESSAGE,
    ):
        # Make sure the service variables are initialized
        if override_folders is None:
            override_folders = []

        # Check if all services in the default variables and overrides directories are defined in the collections
        # If not, raise an error with the list of unknown services
        sources = [
            default_vars_path
            for default_vars_path in self._collections.default_vars_dirs.values()
        ]
        sources += [Path(override) for override in override_folders]
        unknown_services: list[str] = []
        for source_path in sources:
            for service_path in source_path.iterdir():
                if not service_path.is_dir():
                    continue
                service_name = service_path.name
                if service_name not in self:
                    unknown_services.append(service_name)
        if unknown_services:
            raise ValueError(
                f"The following services are not defined in the collections: {', '.join(unknown_services)}"
            )

        # Initialize a dictionary to store services to update
        # Key: service name, Value: ServiceToUpdate instance
        services_to_update: dict[str, ServiceToUpdate] = {}

        # Unlike ClusterVariables.initialize_cluster_variables, update performs a single
        # commit for all changes. Two iterations are required to retrieve the correct
        # informations for the commit message from both the default variables and the
        # overrides directories.

        # Handle default variables directories
        for collection in self._collections._collection_readers:
            default_vars_dir = collection.default_vars_directory

            # Base validation message that will be used for all services in this collection
            collection_validation_msg = self._get_collection_base_validation_msg(
                collection,
            )

            # Add default variables for each service in the collection
            for service_path in default_vars_dir.iterdir():
                # Skip if not a directory
                if not service_path.is_dir():
                    continue

                service_name = service_path.name

                services_to_update.setdefault(
                    service_name,
                    ServiceToUpdate(
                        service_name,
                        base_validation_message=base_validation_msg,
                    ),
                ).add_input_path(service_path, "\n".join(collection_validation_msg))

        # Handle overrides directories
        for overide in override_folders:
            overide = Path(overide)

            # Skip if not a directory
            if not overide.is_dir():
                continue

            # Base validation message that will be used for all services in this override
            overide_base_validation_msg = self._get_override_base_validation_msg(
                overide,
            )

            # Add variables for each service in the override directory
            for service_path in overide.iterdir():
                # Skip if not a directory
                if not service_path.is_dir():
                    continue

                service_name = service_path.name

                validation_msg = [*overide_base_validation_msg]
                # Get custom validation message if it exists
                if custom_validation_msg := self._get_service_custom_validation_msg(
                    service_path,
                    validation_msg_file_name=validation_msg_file_name,
                ):
                    validation_msg.append("User message: " + custom_validation_msg)

                services_to_update.setdefault(
                    service_name,
                    ServiceToUpdate(
                        service_name,
                        base_validation_message=base_validation_msg,
                    ),
                ).add_input_path(
                    service_path,
                    "\n".join(validation_msg),
                )

        # If no services to update, return
        if not services_to_update:
            logger.info("No services to update.")
            return

        # Update each service with the collected input paths
        # Exceptions are collected and raised at the end
        excs = []
        for service_name, service_to_update in services_to_update.items():
            service_variables = self[service_name]
            try:
                # Add input paths to the service variables
                # ? En cas d'erreur, est ce que le repo est dirty ?
                service_variables.update_from_dir(
                    service_to_update.input_paths,
                    validation_message=service_to_update.validation_message,
                )
            except EmptyCommit:
                logger.info(
                    f"Service '{service_name}' will not cause any change, no commit has been made."
                )
            except Exception as e:
                logger.error(
                    f"Error while updating service '{service_name}': {e}",
                    exc_info=True,
                )
                excs.append(e)
        # If there were exceptions, raise them
        if excs:
            raise RuntimeError(
                "Errors occurred while updating services: "
                + ", ".join(str(e) for e in excs)
            )

        # Validate all services schemas if requested
        if validate:
            self._validate_services_schemas()

        logger.info("All services have been updated successfully.")

    def _get_collection_base_validation_msg(
        self,
        collection: CollectionReader,
    ) -> list[str]:
        """Get the base validation message for a collection.

        Args:
            collection: CollectionReader instance.

        Returns:
            List of strings representing the validation message.
        """
        validation_msg = [
            f"Update variables from collection: {collection.name}",
            f"Path: {collection.default_vars_directory.as_posix()}",
        ]
        if galaxy_version := collection.read_galaxy_version():
            validation_msg.append(f"Galaxy collection version: {galaxy_version}")
        if repo := collection.get_repository():
            if not repo.is_clean():
                logger.warning(f"{collection.path} is a repository but is not clean.")
            try:
                repo_version = repo.current_version()
            except NoVersionYet:
                logger.warning(
                    f"{collection.path} is a repository but has no version yet."
                )
                repo_version = "No version yet"
            validation_msg.append(f"Repository version: {repo_version}")
        return validation_msg

    def _get_override_base_validation_msg(
        self,
        override: Path,
        *,
        repository_class: type[Repository] = GitRepository,
    ) -> list[str]:
        """Get the base validation message for an override directory.

        Args:
            override: Path to the override directory.

        Returns:
            List of strings representing the validation message.
        """
        validation_msg = [f"Update variables from override: {override.as_posix()}"]
        if repo := repository_class(override):
            if not repo.is_clean():
                logger.warning(f"{override} is a repository but is not clean.")
            try:
                repo_version = repo.current_version()
            except NoVersionYet:
                logger.warning(f"{override} is a repository but has no version yet.")
                repo_version = "No version yet"
            validation_msg.append(f"Repository version: {repo_version}")
        return validation_msg

    def _get_service_custom_validation_msg(
        self,
        service_path: Path,
        *,
        validation_msg_file_name: str = VALIDATION_MESSAGE_FILE,
    ) -> Optional[str]:
        """Get validation message from the service path."""
        validation_msg_file = service_path / validation_msg_file_name
        try:
            # Read the validation message file
            return validation_msg_file.read_text().strip()
        except PermissionError:
            logger.warning(
                f"Could not read validation message file: {validation_msg_file}"
            )
        except NotADirectoryError:
            logger.error(
                f"Expected a directory for service path, but got a file: {service_path}"
            )
        except FileNotFoundError:
            pass
        return None

    def _validate_services_schemas(self):
        """Validate all services schemas.

        Raises:
            SchemaValidationError: If at least one service schema is invalid.
        """
        errors = []
        for service in self.values():
            try:
                service.validate()
            except SchemaValidationError as e:
                errors.extend(e.errors)
        if errors:
            raise SchemaValidationError(errors)

    def get_modified_entities(
        self,
        entity_statuses: Iterable[HostedEntityStatus],
    ) -> set[HostedEntity]:
        """Get modified entities from a list of hosted entity statuses.

        Args:
            entity_statuses: List of hosted entity statuses.

        Returns:
            Hosted entities that have been modified.

        Raises:
            RuntimeError: If a service is deployed but its repository is missing.
        """
        modified_entities: set[HostedEntity] = set()
        for status in entity_statuses:
            # Skip if the entity has already been listed as modified
            if status.entity in modified_entities:
                continue
            # Raise an error if the service is deployed but its repository is missing
            if status.entity.name.service not in self:
                raise RuntimeError(
                    f"Service '{status.entity.name.service}' is deployed but its"
                    + "repository is missing."
                )
            # Check if the entity has been modified
            if status.configured_version and self[
                status.entity.name.service
            ].is_entity_modified_from_version(
                status.entity.name, status.configured_version
            ):
                logger.debug(
                    f"{status.entity.name} has been modified"
                    + (f" for host {status.entity.host}" if status.entity.host else "")
                )
                modified_entities.add(status.entity)
        return modified_entities
