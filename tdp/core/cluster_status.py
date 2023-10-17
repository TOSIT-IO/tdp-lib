# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Generator, Iterable, Mapping
from typing import TYPE_CHECKING, Any, Optional

from tdp.core.dag import Dag
from tdp.core.models.sch_status_log import SCHStatusLog, SCHStatusLogSourceEnum
from tdp.core.service_component_host_name import ServiceComponentHostName
from tdp.core.service_component_name import ServiceComponentName

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.models import SCHStatusRow
    from tdp.core.variables import ClusterVariables

logger = logging.getLogger(__name__)


# TODO: add latest_update column
class SCHStatus:
    """Status of a service component host."""

    service: str
    component: Optional[str]
    host: Optional[str]
    running_version: Optional[str]
    configured_version: Optional[str]
    to_config: Optional[bool]
    to_restart: Optional[bool]

    def __init__(
        self,
        *,
        service: str,
        component: Optional[str] = None,
        host: Optional[str] = None,
        running_version: Optional[str] = None,
        configured_version: Optional[str] = None,
        to_config: Optional[bool] = None,
        to_restart: Optional[bool] = None,
    ):
        """Initialize a ServiceComponentHostStatus object.

        Args:
            running_version: Running version of the component.
            configured_version: Configured version of the component.
            to_config: True if the component need to be configured.
            to_restart: True if the component need to be restarted.
        """
        self.service = service
        self.component = component
        self.host = host
        self.running_version = running_version
        self.configured_version = configured_version
        self.to_config = to_config
        self.to_restart = to_restart

    @property
    def is_stale(self) -> bool:
        """Whether the service component host is stale."""
        return bool(self.to_config or self.to_restart)

    @staticmethod
    def from_sch_status_row(row: SCHStatusRow, /) -> SCHStatus:
        """Create a SCHStatus from a SCHStatusRow.

        Args:
            row: SCHStatusRow.

        Returns:
            SCHStatus instance."""
        (
            service,
            component,
            host,
            running_version,
            configured_version,
            to_config,
            to_restart,
        ) = row
        return SCHStatus(
            service=service,
            component=component,
            host=host,
            running_version=running_version,
            configured_version=configured_version,
            to_config=bool(to_config),
            to_restart=bool(to_restart),
        )

    # ? transform into a property if we keep it?
    def get_sch_name(self) -> ServiceComponentHostName:
        """Get the service component host name."""
        return ServiceComponentHostName(
            ServiceComponentName(self.service, self.component), self.host
        )

    def update(
        self,
        *,
        running_version: Optional[str] = None,
        configured_version: Optional[str] = None,
        to_config: Optional[bool] = None,
        to_restart: Optional[bool] = None,
    ) -> Optional[SCHStatusLog]:
        """Update the status of a service component host, returns a SCHStatusLog if the status was updated.

        Args:
            running_version: Running version of the component.
            configured_version: Configured version of the component.
            to_config: True if the component need to be configured.
            to_restart: True if the component need to be restarted.

        Returns:
            SCHStatusLog instance if the status was updated, None otherwise.
        """
        # Return early if no update is needed
        if (
            running_version == self.running_version
            and configured_version == self.configured_version
            and to_config == self.to_config
            and to_restart == self.to_restart
        ):
            return

        # Base log
        log = SCHStatusLog(
            service=self.service,
            component=self.component,
            host=self.host,
        )

        if running_version is not None and running_version != self.running_version:
            self.running_version = running_version
            log.running_version = running_version

        if (
            configured_version is not None
            and configured_version != self.configured_version
        ):
            self.configured_version = configured_version
            log.configured_version = configured_version

        if to_config is not None and to_config != self.to_config:
            self.to_config = to_config
            log.to_config = to_config

        if to_restart is not None and to_restart != self.to_restart:
            self.to_restart = to_restart
            log.to_restart = to_restart

        return log

    def to_dict(
        self,
        *,
        filter_out: Optional[list[str]] = None,
        format: Optional[bool] = True,
    ) -> dict[str, Any]:
        """Convert a SCHStatus instance to a dictionary.

        Args:
            filter_out: List of columns to filter out.
            format: Whether to format the values for printing.

        Returns:
            Dictionary representation of the model.
        """
        filter_out = filter_out or []
        return {
            k: self._formater(k, v) if format else v
            for k, v in self.__dict__.items()
            if k not in filter_out
        }

    def _formater(self, key: str, value: Optional[Any], /) -> str:
        """Format a value for printing."""

        if not value:
            return ""
        if key in ["running_version", "configured_version"]:
            value = str(value[:7])
        return str(value)

    def __str__(self) -> str:
        return (
            f"{SCHStatus.__name__}("
            f"service: {self.service}, "
            f"component: {self.component}, "
            f"host: {self.host}, "
            f"running_version: {self.running_version}, "
            f"configured_version: {self.configured_version}, "
            f"to_config: {self.to_config}, "
            f"to_restart: {self.to_restart}"
            f")"
        )

    def __repr__(self) -> str:
        return self.__str__()


class ClusterStatus(Mapping[ServiceComponentHostName, SCHStatus]):
    """Hold what component version are deployed."""

    def __init__(
        self,
        cluster_status_dict: dict[ServiceComponentHostName, SCHStatus],
        /,
    ):
        """Initialize a ClusterStatus object.

        Args:
            cluster_status_dict: Dictionary of ServiceComponentHostName to ServiceComponentStatus.
        """
        self._cluster_status_dict = cluster_status_dict

    def __getitem__(self, key):
        return self._cluster_status_dict.__getitem__(key)

    def __len__(self) -> int:
        return self._cluster_status_dict.__len__()

    def __iter__(self):
        return self._cluster_status_dict.__iter__()

    def items(self):
        return self._cluster_status_dict.items()

    def values(self):
        return self._cluster_status_dict.values()

    def setdefault(self, key, default):
        return self._cluster_status_dict.setdefault(key, default)

    @staticmethod
    def from_sch_status_rows(
        sch_status_rows: Iterable[SCHStatusRow], /
    ) -> ClusterStatus:
        """Get an instance of ClusterStatus from a list of SCHStatusRow.

        Args:
            cluster_status_log: List of SCHStatusRow.

        Returns:
            ClusterStatus instance.
        """
        cluster_status_dict: dict[ServiceComponentHostName, SCHStatus] = {}
        for row in sch_status_rows:
            sch_status = SCHStatus.from_sch_status_row(row)
            cluster_status_dict[sch_status.get_sch_name()] = sch_status
        return ClusterStatus(cluster_status_dict)

    def generate_stale_sch_logs(
        self,
        *,
        cluster_variables: ClusterVariables,
        collections: Collections,
    ) -> set[SCHStatusLog]:
        """Generate SCHStatusLog(s) for components that need to be configured or restarted.

        This method identifies components that have undergone changes in their
        versions and determines if they need to be configured, restarted, or both.

        Note: If a component has neither config or restart operations, it is not
        considered stale and is excluded from the results.

        Args:
            cluster_variables: Current configuration.
            collections: Collections instance.

        Returns:
            Set of SCHStatusLog.
        """
        stale_sch_logs_dict: dict[ServiceComponentHostName, SCHStatusLog] = {}
        source_reconfigure_operations: set[str] = set()

        modified_sch = cluster_variables.get_modified_sch(self.values())

        # Return early if no sch have modified configurations.
        if len(modified_sch) == 0:
            return set()

        for sch in modified_sch:
            sc = sch.service_component_name

            config_operation = collections.get_operation_or_none(sc, "config")
            start_operation = collections.get_operation_or_none(sc, "start")
            restart_operation = collections.get_operation_or_none(sc, "restart")

            # Add the config and start operations to the source_reconfigure_operations set
            # to get the descendants of these operations
            if config_operation:
                source_reconfigure_operations.add(config_operation.name)
            if start_operation:
                source_reconfigure_operations.add(start_operation.name)

            # Create SCHStatusLog for modified sch
            if config_operation or restart_operation:
                stale_sch_log = stale_sch_logs_dict[sch] = SCHStatusLog(
                    service=sc.service_name,
                    component=sc.component_name,
                    host=sch.host_name,
                    source=SCHStatusLogSourceEnum.STALE,
                )
                if config_operation:
                    stale_sch_log.to_config = True
                if start_operation:
                    stale_sch_log.to_restart = True

        # Get the descendants of the reconfigure operations
        dag = Dag(collections)
        operation_descendants = dag.get_operation_descendants(
            nodes=list(source_reconfigure_operations), restart=True
        )

        # Create SCHStatusLog for the descendants
        for operation in operation_descendants:
            # Only consider config and restart operations
            if operation.action_name not in ["config", "restart"]:
                continue

            for host in operation.host_names:
                stale_sch_log = stale_sch_logs_dict.setdefault(
                    ServiceComponentHostName(
                        service_component_name=ServiceComponentName(
                            service_name=operation.service_name,
                            component_name=operation.component_name,
                        ),
                        host_name=host,
                    ),
                    SCHStatusLog(
                        service=operation.service_name,
                        component=operation.component_name,
                        host=host,
                        source=SCHStatusLogSourceEnum.STALE,
                    ),
                )
                if operation.action_name == "config":
                    stale_sch_log.to_config = True
                elif operation.action_name == "restart":
                    stale_sch_log.to_restart = True

        return set(stale_sch_logs_dict.values())

    def find_sch_statuses(
        self,
        *,
        service: Optional[str] = None,
        component: Optional[str] = None,
        hosts: Optional[Iterable[str]] = None,
        stale: Optional[bool] = False,
        to_config: Optional[bool] = None,
        to_restart: Optional[bool] = None,
    ) -> Generator[SCHStatus, None, None]:
        """Find sch statuses based on the given criteria.

        Args:
            service: Service name.
            component: Component name.
            hosts: Host names.
            stale: Whether to return stale sch, i.e. sch that need to be configured or restarted.
            to_config: Whether to return sch that need to be configured.
            to_restart: Whether to return sch that need to be restarted.

        Returns:
            Generator of ServiceComponentHostStatus.
        """
        return (
            sch_status
            for sch_status in self.values()
            if (
                (not service or sch_status.service == service)
                and (not component or sch_status.component == component)
                and (not hosts or sch_status.host in list(hosts))
                and (not stale or sch_status.to_config or sch_status.to_restart)
                and (to_config is None or sch_status.to_config is to_config)
                and (to_restart is None or sch_status.to_restart is to_restart)
            )
        )

    def update_sch(
        self,
        sch: ServiceComponentHostName,
        /,
        *,
        action_name: str,
        version: Optional[str] = None,
        can_update_stale: bool = False,
    ) -> Optional[SCHStatusLog]:
        """Update the status of a sch, returns a log if the status was updated.

        Args:
            sch: sch to update.
            action_name: Action name of the ongoing operation (i.e. config, start...).
            version: Version to update to.
            update_stale: Whether to update to_config and to_update values.

        Returns:
            ClusterStatusLog if the status was updated, None otherwise.
        """
        service = sch.service_component_name.service_name
        component = sch.service_component_name.component_name

        sch_status = self.setdefault(
            sch,
            SCHStatus(
                service=service,
                component=component,
                host=sch.host_name,
            ),
        )

        if action_name == "config":
            return sch_status.update(
                configured_version=version,
                to_config=False if can_update_stale else None,
            )

        if action_name == "restart":
            return sch_status.update(
                running_version=version,
                to_restart=False if can_update_stale else None,
            )

        if action_name == "start":
            if not sch_status.configured_version:
                return
            return sch_status.update(
                running_version=sch_status.configured_version,
            )

    def is_sc_stale(
        self, sc_name: ServiceComponentName, /, sc_hosts: Optional[Iterable[str]]
    ) -> bool:
        """Whether a service or component is stale.

        Args:
            sc_name: Service component name to check.
            sc_hosts: Host names where the service component may be deployed.

        Returns:
            True if the service component is stale, False otherwise.
        """
        for host in sc_hosts or [None]:
            sch_status = self.get(ServiceComponentHostName(sc_name, host), None)
            if sch_status and sch_status.is_stale:
                return True
        return False
