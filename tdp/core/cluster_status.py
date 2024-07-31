# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable, MutableMapping
from typing import TYPE_CHECKING, Optional

from tdp.core.entities.hostable_entity_name import (
    HostableEntityName,
)
from tdp.core.entities.hosted_entity import (
    HostedEntity,
    create_hosted_entity,
)
from tdp.core.entities.hosted_entity_status import HostedEntityStatus
from tdp.core.models.sch_status_log_model import (
    SCHStatusLogModel,
)

if TYPE_CHECKING:
    pass

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
        self, entity_name: HostableEntityName, /, hosts: Optional[Iterable[str]]
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
