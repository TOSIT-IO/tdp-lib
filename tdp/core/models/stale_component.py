# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging

from sqlalchemy import Boolean, Column, String

from tdp.core.dag import Dag
from tdp.core.models.base import Base
from tdp.core.models.component_version_log import ComponentVersionLog
from tdp.core.operation import COMPONENT_NAME_MAX_LENGTH, SERVICE_NAME_MAX_LENGTH
from tdp.core.variables import ClusterVariables

logger = logging.getLogger("tdp").getChild("stale_component")


class StaleComponent(Base):
    """Hold what components are staled.

    Attributes:
        service_name (str): Service name.
        component_name (str): Component name.
        to_reconfigure (bool): Is configured flag.
        to_restart (bool): Is restarted flag.
    """

    __tablename__ = "stale_component"

    service_name = Column(
        String(length=SERVICE_NAME_MAX_LENGTH), primary_key=True, nullable=False
    )
    component_name = Column(
        String(length=COMPONENT_NAME_MAX_LENGTH), primary_key=True, nullable=True
    )
    to_reconfigure = Column(Boolean, default=False)
    to_restart = Column(Boolean, default=False)

    @staticmethod
    def generate(
        dag: Dag,
        cluster_variables: ClusterVariables,
        deployed_component_version_logs: ComponentVersionLog,
    ) -> list[StaleComponent]:
        modified_services_or_components_names = (
            cluster_variables.get_modified_components_names(
                services_components_versions=deployed_component_version_logs
            )
        )
        modified_components_names = set()
        for modified_component_name in modified_services_or_components_names:
            logger.debug(f"{modified_component_name.full_name} has been modified.")
            if modified_component_name.is_service:
                config_operations_of_the_service = filter(
                    lambda operation: operation.action_name == "config"
                    and not operation.is_service_operation(),
                    dag.services_operations[modified_component_name.service_name],
                )
                modified_components_names.update(
                    [operation.name for operation in config_operations_of_the_service]
                )
            else:
                modified_components_names.add(
                    f"{modified_component_name.full_name}_config"
                )
        if not modified_components_names:
            return []
        operations = dag.get_operations(
            sources=list(modified_components_names), restart=True
        )
        config_and_restart_operations = dag.filter_operations_regex(
            operations, r".+_(config|(re|)start)"
        )
        stale_components_dict = {}
        for operation in config_and_restart_operations:
            if operation.is_service_operation():
                continue
            key = (operation.service_name, operation.component_name)
            stale_component = stale_components_dict.setdefault(
                key,
                StaleComponent(
                    service_name=operation.service_name,
                    component_name=operation.component_name,
                    to_reconfigure=False,
                    to_restart=False,
                ),
            )
            if operation.action_name == "config":
                stale_component.to_reconfigure = True
            if operation.action_name == "restart":
                stale_component.to_restart = True
        return list(stale_components_dict.values())

    @staticmethod
    def to_dict(
        stale_components: list[StaleComponent],
    ) -> dict[tuple[str, str], StaleComponent]:
        """Convert the list of stale components to a dictionary.

        Args:
            stale_components: The list of stale components to convert.

        Returns:
            The dictionary of stale components.
        """
        return {
            (
                stale_component.service_name,
                stale_component.component_name,
            ): stale_component
            for stale_component in stale_components
        }
