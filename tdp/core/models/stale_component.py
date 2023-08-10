# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from tdp.core.models.base import Base
from tdp.core.operation import (
    COMPONENT_NAME_MAX_LENGTH,
    HOST_NAME_MAX_LENGTH,
    SERVICE_NAME_MAX_LENGTH,
)

if TYPE_CHECKING:
    from tdp.core.dag import Dag
    from tdp.core.models.component_version_log import ComponentVersionLog
    from tdp.core.variables import ClusterVariables


class StaleComponent(Base):
    """Hold what components are staled.

    Attributes:
        service_name: Service name.
        component_name: Component name.
        host_name: Host name.
        to_reconfigure: Is configured flag.
        to_restart: Is restarted flag.
    """

    __tablename__ = "stale_component"

    service_name: Mapped[str] = mapped_column(
        String(SERVICE_NAME_MAX_LENGTH), primary_key=True
    )
    component_name: Mapped[str] = mapped_column(
        String(COMPONENT_NAME_MAX_LENGTH), primary_key=True
    )
    host_name: Mapped[str] = mapped_column(
        String(HOST_NAME_MAX_LENGTH), primary_key=True
    )
    to_reconfigure: Mapped[Optional[bool]]
    to_restart: Mapped[Optional[bool]]

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
            # Noop operation don't have any host
            if not any(operation.host_names):
                stale_component = stale_components_dict.setdefault(
                    (operation.service_name, "", ""),
                    StaleComponent(
                        service_name=operation.service_name,
                        component_name=operation.component_name or "",
                        host_name="",
                        to_reconfigure=False,
                        to_restart=False,
                    ),
                )
                if operation.action_name == "config":
                    stale_component.to_reconfigure = True
                if operation.action_name == "restart":
                    stale_component.to_restart = True
            for host_name in operation.host_names:
                key = (operation.service_name, operation.component_name, host_name)
                stale_component = stale_components_dict.setdefault(
                    key,
                    StaleComponent(
                        service_name=operation.service_name,
                        component_name=operation.component_name or "",
                        host_name=host_name,
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
    ) -> dict[tuple[str, str, str], StaleComponent]:
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
                stale_component.host_name,
            ): stale_component
            for stale_component in stale_components
        }
