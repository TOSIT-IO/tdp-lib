# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime

from tdp.core.collections import Collections
from tdp.core.deployment import DeploymentPlan
from tdp.core.deployment.executor import Executor
from tdp.core.models import (
    OperationLog,
    DeploymentStateEnum,
    OperationStateEnum,
)
from tdp.core.operation import Operation
from tdp.core.variables import ClusterVariables

from .deployment_iterator import DeploymentIterator

logger = logging.getLogger("tdp").getChild("deployment_runner")


class DeploymentRunner:
    """Allows to get an iterator from a deployment plan."""

    def __init__(
        self,
        collections: Collections,
        executor: Executor,
        cluster_variables: ClusterVariables,
    ):
        """Deployment runner.

        Args:
            collections: Collections object.
            executor: Executor object.
            cluster_variables: ClusterVariables object.
        """
        self._collections = collections
        self._executor = executor
        self._cluster_variables = cluster_variables

    def _run_operation(self, operation: Operation) -> OperationLog:
        """Run operation.

        Args:
            operation: Operation to be run.

        Returns:
            OperationLog object with the operation's logs.
        """
        logger.debug(f"Running operation {operation.name}")

        start = datetime.utcnow()

        operation_file = self._collections[operation.collection_name].playbooks[
            operation.name
        ]
        state, logs = self._executor.execute(operation_file)
        end = datetime.utcnow()

        if not OperationStateEnum.has_value(state):
            logger.error(
                f"Invalid state ({state}) returned by {self._executor.__class__.__name__}.run('{operation_file}'))"
            )
            state = OperationStateEnum.FAILURE
        elif not isinstance(state, OperationStateEnum):
            state = OperationStateEnum(state)

        return OperationLog(
            operation_order=1,
            operation=operation.name,
            start_time=start,
            end_time=end,
            state=state,
            logs=logs,
        )

    def run(self, deployment_plan: DeploymentPlan) -> DeploymentIterator:
        """Provides an iterator to run a deployment plan.

        Args:
            deployment_plan: Deployment plan to be run.

        Returns:
            DeploymentIterator object.
        """
        deployment_plan.deployment_log.state = DeploymentStateEnum.RUNNING
        return DeploymentIterator(
            deployment_plan.deployment_log,
            deployment_plan.operations,
            self._run_operation,
            self._cluster_variables,
        )
