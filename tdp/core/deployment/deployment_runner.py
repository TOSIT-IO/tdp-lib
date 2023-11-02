# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from tdp.core.deployment.deployment_iterator import DeploymentIterator
from tdp.core.models import DeploymentStateEnum, OperationStateEnum
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.cluster_status import ClusterStatus
    from tdp.core.collections import Collections
    from tdp.core.deployment.executor import Executor
    from tdp.core.models import DeploymentModel, OperationLog

logger = logging.getLogger(__name__)


class DeploymentRunner:
    """Allows to get an iterator from a deployment plan."""

    def __init__(
        self,
        executor: Executor,
        collections: Collections,
        cluster_variables: ClusterVariables,
        cluster_status: ClusterStatus,
    ):
        """Deployment runner.

        Args:
            collections: Collections object.
            executor: Executor object.
            cluster_variables: ClusterVariables object.
            stale_components: List of stale components to actualize.
        """
        self._collections = collections
        self._executor = executor
        self._cluster_variables = cluster_variables
        self._cluster_status = cluster_status

    def _run_operation(self, operation_log: OperationLog) -> None:
        """Run operation.

        Args:
            operation_log: Operation to run, modified in place with the result.
        """
        operation_log.start_time = datetime.utcnow()

        operation = self._collections.get_operation(operation_log.operation)

        # Check if the operation is available for the given host
        if operation_log.host and operation_log.host not in operation.host_names:
            logs = (
                f"Operation '{operation_log.operation}' not available for host "
                + f"'{operation_log.host}'"
            )
            logger.error(logs)
            operation_log.state = OperationStateEnum.FAILURE
            operation_log.logs = logs.encode("utf-8")
            operation_log.end_time = datetime.utcnow()
            return

        # Execute the operation
        playbook_file = self._collections[operation.collection_name].playbooks[
            operation.name
        ]
        state, logs = self._executor.execute(
            playbook=playbook_file,
            host=operation_log.host,
            extra_vars=operation_log.extra_vars,
        )
        operation_log.end_time = datetime.utcnow()

        # ? This case shouldn't happen as the executor should return a valid state
        if state not in OperationStateEnum:
            logger.error(
                f"Invalid state ({state}) returned by {self._executor.__class__.__name__}.run('{playbook_file}'))"
            )
            state = OperationStateEnum.FAILURE
        elif not isinstance(state, OperationStateEnum):
            state = OperationStateEnum(state)
        operation_log.state = state
        operation_log.logs = logs

    def run(
        self,
        deployment: DeploymentModel,
        *,
        force_stale_update: bool = False,
    ) -> DeploymentIterator:
        """Provides an iterator to run a deployment plan.

        Args:
            deployment: deployment to run.
            force_sch_update: Force SCH status update.

        Returns:
            DeploymentIterator object, to iterate over operations logs.
        """
        deployment.status = DeploymentStateEnum.RUNNING
        return DeploymentIterator(
            deployment=deployment,
            collections=self._collections,
            run_method=self._run_operation,
            cluster_variables=self._cluster_variables,
            cluster_status=self._cluster_status,
            force_stale_update=force_stale_update,
        )
