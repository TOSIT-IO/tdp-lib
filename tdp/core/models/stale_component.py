# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from tdp.core.collections import MissingOperationError
from tdp.core.models.base import Base
from tdp.core.operation import (
    COMPONENT_NAME_MAX_LENGTH,
    HOST_NAME_MAX_LENGTH,
    SERVICE_NAME_MAX_LENGTH,
)

if TYPE_CHECKING:
    from tdp.core.dag import Dag
    from tdp.core.models.component_version_log import ComponentVersionLog
    from tdp.core.operation import Operation
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
        """
        Generate a list of components that need config or restart.

        This method identifies components that have undergone changes in their
        versions and determines if they need to be configured, restarted, or both.

        Note: If a component has neither config or restart operations, it is not
        considered stale and is excluded from the results.

        Args:
            dag: The DAG representing dependencies between operations.
            cluster_variables: Current configuration.
            deployed_component_version_logs: Logs capturing versions of previously
              deployed components.

        Returns:
            List of components that need configuration or restart.

        Raises:
            MissingOperationError if a particular operation associated with a
              component does not exist in the DAG.
        """
        # Return early if there are no previously deployed components
        # (indicating an empty cluster).
        if len(deployed_component_version_logs) == 0:
            return []

        # Identify components that have modified configurations.
        modified_components = cluster_variables.get_modified_service_components(
            deployed_component_version_logs
        )

        # Return early if no components have modified configurations.
        if len(modified_components) == 0:
            return []

        # Retrieve config and start operations for components with modifications.
        config_start_modified_operations: set[Operation] = set()
        for modified_component in modified_components:
            try:
                config_start_modified_operations.add(
                    dag.collections.get_operation(
                        modified_component.full_name + "_config"
                    )
                )
            except MissingOperationError:
                pass

            try:
                config_start_modified_operations.add(
                    dag.collections.get_operation(
                        modified_component.full_name + "_start"
                    )
                )
            except MissingOperationError:
                pass

        stale_components_host_dict: dict[tuple[str, str, str], StaleComponent] = {}
        # Identify config and restart operations directly associated with
        # modified components for specific hosts.
        for modified_component in modified_components:
            service_name = modified_component.service_component_name.service_name
            component_name = (
                modified_component.service_component_name.component_name or ""
            )
            host_name = modified_component.host_name or ""

            # Attempt to find related config and restart operations for the component.
            config_operation = None
            try:
                config_operation = dag.collections.get_operation(
                    modified_component.full_name + "_config"
                )
            except MissingOperationError:
                pass

            restart_operation = None
            try:
                restart_operation = dag.collections.get_operation(
                    modified_component.full_name + "_restart"
                )
            except MissingOperationError:
                pass

            # Create a StaleComponent entry if either operation exists.
            if config_operation or restart_operation:
                stale_component = StaleComponent(
                    service_name=service_name,
                    component_name=component_name,
                    host_name=host_name,
                )
                if config_operation:
                    stale_component.to_reconfigure = True
                if restart_operation:
                    stale_component.to_restart = True
                stale_components_host_dict[
                    (service_name, component_name, host_name)
                ] = stale_component

        # Identify descendants in the DAG for the configuration operations.
        # A reconfigure is made of a config and restart operations.
        operation_descendants = dag.get_operation_descendants(
            nodes=[operation.name for operation in config_start_modified_operations],
            restart=True,
        )

        # Determine actions (config or restart) for the descendants of
        # modified components.
        for operation_descendant in operation_descendants:
            # Consider only config and restart action
            if (
                not operation_descendant.action_name == "config"
                and not operation_descendant.action_name == "restart"
            ):
                continue

            service_name = operation_descendant.service_name or ""
            component_name = operation_descendant.component_name or ""
            host_names = operation_descendant.host_names or [""]
            for host_name in host_names:
                stale_component = stale_components_host_dict.setdefault(
                    (service_name, component_name, host_name),
                    StaleComponent(
                        service_name=service_name,
                        component_name=component_name,
                        host_name=host_name,
                    ),
                )
                if operation_descendant.action_name == "config":
                    stale_component.to_reconfigure = True
                if operation_descendant.action_name == "restart":
                    stale_component.to_restart = True

        # Return the list of stale components, sorted lexicographically on hosts.
        return sorted(
            stale_components_host_dict.values(),
            key=lambda x: f"{x.service_name}_{x.component_name}_{x.host_name}",
        )

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
