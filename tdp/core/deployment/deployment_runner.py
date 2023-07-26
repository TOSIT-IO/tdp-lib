# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime

from tdp.core.collections import Collections
from tdp.core.deployment.executor import Executor
from tdp.core.models import (
    DeploymentLog,
    DeploymentStateEnum,
    OperationLog,
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

    def _run_operation(self, operation_log: OperationLog):
        """Run operation.

        Args:
            operation_log: Operation to run, modified in place with the result.
        """
        logger.debug(f"Running operation {operation_log.operation}")

        operation_log.start_time = datetime.utcnow()

        operation = self._collections.get_operation(operation_log.operation)
        operation_file = self._collections[operation.collection_name].playbooks[
            operation.name
        ]
        state, logs = self._executor.execute(operation_file)
        operation_log.end_time = datetime.utcnow()

        if not OperationStateEnum.has_value(state):
            logger.error(
                f"Invalid state ({state}) returned by {self._executor.__class__.__name__}.run('{operation_file}'))"
            )
            state = OperationStateEnum.FAILURE
        elif not isinstance(state, OperationStateEnum):
            state = OperationStateEnum(state)
        operation_log.state = state
        operation_log.logs = logs

    def run(self, deployment_log: DeploymentLog) -> DeploymentIterator:
        """Provides an iterator to run a deployment plan.

        Args:
            deployment_log: Deployment log to run.

        Returns:
            DeploymentIterator object, to iterate over operations logs.
        """
        deployment_log.state = DeploymentStateEnum.RUNNING
        return DeploymentIterator(
            deployment_log=deployment_log,
            collections=self._collections,
            run_method=self._run_operation,
            cluster_variables=self._cluster_variables,
        )
