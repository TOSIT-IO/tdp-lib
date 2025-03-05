# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable, MutableMapping
from typing import TYPE_CHECKING, Optional

from tdp.core.dag import Dag
from tdp.core.entities.entity_name import (
    EntityName,
    create_entity_name,
)
from tdp.core.entities.hosted_entity import (
    HostedEntity,
    HostedServiceComponent,
    create_hosted_entity,
)
from tdp.core.entities.hosted_entity_status import HostedEntityStatus
from tdp.core.entities.operation import PlaybookOperation
from tdp.core.models.sch_status_log_model import (
    SCHStatusLogModel,
    SCHStatusLogSourceEnum,
)

if TYPE_CHECKING:
    from tdp.core.collections.collections import Collections
    from tdp.core.variables.cluster_variables import ClusterVariables

logger = logging.getLogger(__name__)


class ClusterStatus(MutableMapping[HostedEntity, HostedEntityStatus]):
    """Holds the status of all hosted entities in the cluster.

    The ClusterStatus object is used to keep track of the status of all hosted entities.
    It provides methods to update the status of an entity and to generate logs for
    stale entities.
    """

    def __init__(self, hosted_entity_statuses: Iterable[HostedEntityStatus]):
        """Initialize the ClusterStatus object."""
        self._cluster_status = {}
        for status in hosted_entity_statuses:
            self._cluster_status[status.entity] = status

    def __getitem__(self, key: HostedEntity) -> HostedEntityStatus:
        return self._cluster_status.__getitem__(key)

    def __setitem__(self, key: HostedEntity, value: HostedEntityStatus):
        return self._cluster_status.__setitem__(key, value)

    def __delitem__(self, key: HostedEntity):
        return self._cluster_status.__delitem__(key)

    def __len__(self) -> int:
        return self._cluster_status.__len__()

    def __iter__(self):
        return self._cluster_status.__iter__()

    def generate_stale_sch_logs(
        self,
        *,
        cluster_variables: ClusterVariables,
        collections: Collections,
    ) -> set[SCHStatusLogModel]:
        """Generate logs for components that need to be configured or restarted.

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
        logs: dict[HostedEntity, SCHStatusLogModel] = {}
        source_reconfigure_operations: set[str] = set()

        modified_entities = cluster_variables.get_modified_entities(self.values())

        # Return early if no entity has modified configurations
        if len(modified_entities) == 0:
            return set()

        # Create logs for the modified entities
        for entity in modified_entities:
            config_operation = collections.operations.get(f"{entity.name}_config")
            start_operation = collections.operations.get(f"{entity.name}_start")
            restart_operation = collections.operations.get(f"{entity.name}_restart")

            # Add the config and start operations to the set to get their descendants
            if config_operation:
                source_reconfigure_operations.add(config_operation.name.name)
            if start_operation:
                source_reconfigure_operations.add(start_operation.name.name)

            # Create a log to update the stale status of the entity if a config and/or
            # restart operations are available
            # Only source hosts affected by the modified configuration are considered as
            # stale (while all hosts are considered as stale for the descendants)
            if config_operation or restart_operation:
                log = logs.setdefault(
                    entity,
                    SCHStatusLogModel(
                        service=entity.name.service,
                        component=(
                            entity.name.component
                            if isinstance(entity, HostedServiceComponent)
                            else None
                        ),
                        host=entity.host,
                        source=SCHStatusLogSourceEnum.STALE,
                    ),
                )
                if config_operation:
                    log.to_config = True
                if restart_operation:
                    log.to_restart = True

        # Create logs for the descendants of the modified entities
        for operation in Dag(collections).get_operation_descendants(
            nodes=list(source_reconfigure_operations), restart=True
        ):
            # Only create a log when config or restart operation is available
            if operation.name.action not in ["config", "restart"]:
                continue

            # Create a log for each host where the entity is deployed
            for host in (
                operation.playbook.hosts
                if isinstance(operation, PlaybookOperation)
                else []
            ):
                log = logs.setdefault(
                    create_hosted_entity(
                        create_entity_name(
                            operation.name.service, operation.name.component
                        ),
                        host,
                    ),
                    SCHStatusLogModel(
                        service=operation.name.service,
                        component=operation.name.component,
                        host=host,
                        source=SCHStatusLogSourceEnum.STALE,
                    ),
                )
                if operation.name.action == "config":
                    log.to_config = True
                elif operation.name.action == "restart":
                    log.to_restart = True

        return set(logs.values())

    def update_hosted_entity(
        self,
        entity: HostedEntity,
        /,
        *,
        action_name: str,
        version: Optional[str] = None,
        can_update_stale: bool = False,
    ) -> Optional[SCHStatusLogModel]:
        """Update the status of a sch, returns a log if the status was updated.

        Args:
            entity: hosted entity to update
            action_name: Action name of the ongoing operation (i.e. config, start...).
            version: Version to update to.
            can_update_stale: Whether to update to_config and to_update values.

        Returns:
            SCHStatusLogModel if the status was updated, None otherwise.
        """

        status = self.setdefault(entity, HostedEntityStatus(entity))

        if action_name == "config":
            return status.update(
                configured_version=version,
                to_config=False if can_update_stale else None,
            )

        if action_name == "restart":
            return status.update(
                running_version=version,
                to_restart=False if can_update_stale else None,
            )

        if action_name == "start":
            if not status.configured_version:
                return
            return status.update(
                running_version=status.configured_version,
            )

    def is_sc_stale(
        self, entity_name: EntityName, /, hosts: Optional[Iterable[str]]
    ) -> bool:
        """Whether a service or component is stale.

        Args:
            sc_name: Service component name to check.
            sc_hosts: Host names where the service component may be deployed.

        Returns:
            True if the service component is stale, False otherwise.
        """
        for host in hosts or [None]:
            if (
                status := self.get(create_hosted_entity(entity_name, host))
            ) and status.is_stale:
                return True
        return False
