# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from tdp.core.deployment.deployment_iterator import DeploymentIterator
from tdp.core.models.enums import DeploymentStateEnum, OperationStateEnum
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.cluster_status import ClusterStatus
    from tdp.core.collections import Collections
    from tdp.core.deployment.executor import Executor
    from tdp.core.models import DeploymentModel, OperationModel

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
            stale_hosted_entoty_statuses: List of stale hosted entity statuses.
        """
        self._collections = collections
        self._executor = executor
        self._cluster_variables = cluster_variables
        self._cluster_status = cluster_status

    def _run_operation(self, operation_rec: OperationModel) -> None:
        """Run operation.

        Args:
            operation_rec: Operation record to run, modified in place with the result.
        """
        operation_rec.start_time = datetime.utcnow()

        operation = self._collections.operations[operation_rec.operation]

        # Check if the operation is available for the given host
        if operation_rec.host and operation_rec.host not in operation.host_names:
            logs = (
                f"Operation '{operation_rec.operation}' not available for host "
                + f"'{operation_rec.host}'"
            )
            logger.error(logs)
            operation_rec.state = OperationStateEnum.FAILURE
            operation_rec.logs = logs.encode("utf-8")
            operation_rec.end_time = datetime.utcnow()
            return

        # Execute the operation
        playbook_file = self._collections.playbooks[operation.name.name].path
        state, logs = self._executor.execute(
            playbook=playbook_file,
            host=operation_rec.host,
            extra_vars=operation_rec.extra_vars,
        )
        operation_rec.end_time = datetime.utcnow()

        # ? This case shouldn't happen as the executor should return a valid state
        if state not in OperationStateEnum:
            logger.error(
                f"Invalid state ({state}) returned by {self._executor.__class__.__name__}.run('{playbook_file}'))"
            )
            state = OperationStateEnum.FAILURE
        elif not isinstance(state, OperationStateEnum):
            state = OperationStateEnum(state)
        operation_rec.state = state
        operation_rec.logs = logs

    def run(
        self,
        deployment: DeploymentModel,
        *,
        force_stale_update: bool = False,
    ) -> DeploymentIterator:
        """Provides an iterator to run a deployment plan.

        Args:
            deployment: Deployment to run.
            force_sch_update: Force SCH status update.

        Returns:
            DeploymentIterator object, to iterate over operations logs.
        """
        deployment.state = DeploymentStateEnum.RUNNING
        return DeploymentIterator(
            deployment=deployment,
            collections=self._collections,
            run_method=self._run_operation,
            cluster_variables=self._cluster_variables,
            cluster_status=self._cluster_status,
            force_stale_update=force_stale_update,
        )
