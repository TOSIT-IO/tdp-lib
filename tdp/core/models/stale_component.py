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


class StaleComponent(Base):
    """Hold what components are staled.

    Attributes:
        service_name (str): Service name.
        component_name (str): Component name.
        to_reconfigure (bool): Is configured flag.
        to_restart (bool): Is restarted flag.
    """

    __tablename__ = "stale_component"

    service_name = Column(String(length=SERVICE_NAME_MAX_LENGTH), primary_key=True)
    component_name = Column(String(length=COMPONENT_NAME_MAX_LENGTH), primary_key=True)
    to_reconfigure = Column(Boolean, default=False)
    to_restart = Column(Boolean, default=False)

    @staticmethod
    def generate(
        dag: Dag,
        cluster_variables: ClusterVariables,
        deployed_component_version_logs: list[ComponentVersionLog],
    ) -> list[StaleComponent]:
        # Nothing is deployed (empty cluster)
        if len(deployed_component_version_logs) == 0:
            return []
        modified_services_components_names = (
            cluster_variables.get_modified_services_components_names(
                deployed_component_version_logs
            )
        )
        # No configuration have been modified (clean cluster)
        if len(modified_services_components_names) == 0:
            return []
        sources_config_operations = [
            service_component_name.full_name + "_config"
            for service_component_name in modified_services_components_names
        ]
        #  When a service is modified, extend operation list with its components
        for modified_service_component_name in modified_services_components_names:
            if modified_service_component_name.is_service:
                service_operations = filter(
                    lambda operation: operation.action_name == "config",
                    dag.services_operations[
                        modified_service_component_name.service_name
                    ],
                )
                sources_config_operations.extend(
                    [service_operation.name for service_operation in service_operations]
                )
        operations = dag.get_operations(sources=sources_config_operations, restart=True)
        config_and_restart_operations = dag.filter_operations_regex(
            operations, r".+_(config|restart)"
        )
        stale_components_dict = {}
        for operation in config_and_restart_operations:
            key = (operation.service_name, operation.component_name)
            stale_component = stale_components_dict.setdefault(
                key,
                StaleComponent(
                    service_name=operation.service_name,
                    component_name=operation.component_name
                    if not operation.is_service_operation()
                    else "",
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
