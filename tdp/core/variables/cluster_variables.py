# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from tdp.core.constants import DEFAULT_VALIDATION_MESSAGE, VALIDATION_MESSAGE_FILE
from tdp.core.repository.git_repository import GitRepository
from tdp.core.repository.repository import EmptyCommit, NoVersionYet, Repository
from tdp.core.types import PathLike
from tdp.core.variables.exceptions import (
    ServicesNotInitializedError,
    ServicesUpdateError,
    UnknownService,
    UpdateError,
)
from tdp.core.variables.messages import ValidationMessageBuilder
from tdp.core.variables.planner import ServiceUpdatePlanner
from tdp.core.variables.scanner import ServiceDirectoryScanner
from tdp.core.variables.schema.exceptions import SchemaValidationError
from tdp.core.variables.service_variables import ServiceVariables

if TYPE_CHECKING:
    from tdp.core.collections.collections import Collections
    from tdp.core.repository.repository import Repository
logger = logging.getLogger(__name__)


class ClusterVariables(Mapping[str, ServiceVariables]):
    """Mapping of service names to their ServiceVariables instances."""

    def __init__(
        self,
        service_variables_dict: dict[str, ServiceVariables],
        collections: Collections,
    ):
        self._service_variables_dict = service_variables_dict
        self._collections = collections

    def __getitem__(self, key):
        return self._service_variables_dict.__getitem__(key)

    def __len__(self) -> int:
        return self._service_variables_dict.__len__()

    def __iter__(self):
        return self._service_variables_dict.__iter__()

    @classmethod
    def initialize_cluster_variables(
        cls,
        collections: Collections,
        tdp_vars: PathLike,
        override_folders: Optional[Iterable[PathLike]] = None,
        validate: bool = False,
    ) -> ClusterVariables:
        """Initializes ClusterVariables by applying overrides and performing commits per input path."""
        tdp_vars = Path(tdp_vars)
        override_folders = override_folders or []

        current = cls.get_cluster_variables(collections, tdp_vars)
        new_variables: dict[str, ServiceVariables] = {}

        validation_builder = ValidationMessageBuilder(collections)
        planner = ServiceUpdatePlanner(collections, validation_builder)
        sources = [
            (name, path) for name, path in collections.default_vars_dirs.items()
        ] + [("override", Path(p)) for p in override_folders]
        plans = planner.plan_updates(sources, merge_inputs=False)

        for plan in plans:
            service_name = plan.service_name
            if service_name in current:
                try:
                    # Raise NoVersionYet if no commit has been made yet
                    current[service_name].version
                    logger.info(f"{service_name} already initialized")
                    continue
                except NoVersionYet:
                    pass

            service_vars = new_variables.setdefault(
                service_name,
                ServiceVariables.from_path(
                    tdp_vars / service_name,
                    schema=collections.schemas.get(service_name),
                ),
            )
            try:
                service_vars.update_from_dir(
                    plan.input_paths,
                    validation_message=plan.validation_message,
                )
                logger.info(
                    f"{service_name} successfully updated from paths: {[str(p) for p in plan.input_paths]}"
                )
            except EmptyCommit:
                logger.info(f"No change detected for {service_name}.")
                pass

        result = cls(new_variables, collections)
        if validate:
            result._validate_services_schemas()
        return result

    @staticmethod
    def get_cluster_variables(
        collections: Collections,
        tdp_vars: PathLike,
        repository_class: type[Repository] = GitRepository,
        validate=False,
    ):
        """Load all existing ServiceVariables from the given tdp_vars directory."""
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
        validate: bool = False,
        *,
        validation_msg_file_name: str = VALIDATION_MESSAGE_FILE,
        base_validation_msg: str = DEFAULT_VALIDATION_MESSAGE,
    ):
        """Update existing ServiceVariables using override folders, one commit per service."""
        override_folders = override_folders or []

        sources = [
            (name, path) for name, path in self._collections.default_vars_dirs.items()
        ] + [("override", Path(p)) for p in override_folders]

        unknown = []
        for _, source_path in sources:
            for name, _ in ServiceDirectoryScanner.scan(source_path).items():
                if name not in self:
                    unknown.append(UnknownService(name, source_path.as_posix()))
        if unknown:
            raise ServicesNotInitializedError(unknown)

        validation_builder = ValidationMessageBuilder(
            self._collections,
            validation_msg_file_name=validation_msg_file_name,
        )
        planner = ServiceUpdatePlanner(self._collections, validation_builder)
        plans = planner.plan_updates(sources, merge_inputs=True)

        errors = []
        for plan in plans:
            msg = base_validation_msg + "\n" + plan.validation_message
            try:
                self[plan.service_name].update_from_dir(
                    plan.input_paths,
                    validation_message=msg,
                    clear=True,
                )
                logger.info(
                    f"{plan.service_name} successfully updated from paths: {[str(p) for p in plan.input_paths]}"
                )
            except EmptyCommit:
                logger.info(f"No change deteted for {plan.service_name}.")
            except Exception as e:
                logger.error(f"Update failed for {plan.service_name}: {e}")
                errors.append(UpdateError(plan.service_name, str(e)))
        if errors:
            raise ServicesUpdateError(errors)

        if validate:
            self._validate_services_schemas()

        return self

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
