# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime

from tdp.core.collections import Collections
from tdp.core.deployment import DeploymentPlan
from tdp.core.deployment.executor import Executor
from tdp.core.models import DeploymentLog, OperationLog, StateEnum
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

        operation_file = self._collections[operation.collection_name].operations[
            operation.name
        ]
        state, logs = self._executor.execute(operation_file)
        end = datetime.utcnow()

        if not StateEnum.has_value(state):
            logger.error(
                f"Invalid state ({state}) returned by {self._executor.__class__.__name__}.run('{operation_file}'))"
            )
            state = StateEnum.FAILURE
        elif not isinstance(state, StateEnum):
            state = StateEnum(state)

        return OperationLog(
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
        deployment_log = DeploymentLog(
            state=StateEnum.PENDING,
            **deployment_plan.deployment_args,
        )
        return DeploymentIterator(
            deployment_log,
            deployment_plan.operations,
            self._run_operation,
            self._cluster_variables,
        )
