# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tdp.core.entities.hosted_entity import HostedEntity
    from tdp.core.entities.hosted_entity_status import HostedEntityStatus
    from tdp.core.variables.cluster_variables import ClusterVariables

logger = logging.getLogger(__name__)


def get_modified_entities(
    cluster_variables: ClusterVariables,
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
        if status.entity.name.service not in cluster_variables:
            raise RuntimeError(
                f"Service '{status.entity.name.service}' is deployed but its"
                + "repository is missing."
            )
        # Check if the entity has been modified
        if status.configured_version and cluster_variables[
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
