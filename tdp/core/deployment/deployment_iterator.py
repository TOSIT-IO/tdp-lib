# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime
from typing import Iterator, List, Callable, Tuple

from tdp.core.models import (
    DeploymentLog,
    DeploymentStateEnum,
    OperationLog,
    OperationStateEnum,
    ServiceComponentLog,
)
from tdp.core.operation import Operation
from tdp.core.variables import ClusterVariables


class _Flags:
    """Class to store the status of a component.

    Attributes:
        is_configured: True if the component is configured.
        is_started: True if the component is started.
    """

    def __init__(self, is_configured=False, is_started=False):
        self.is_configured = is_configured
        self.is_started = is_started


class DeploymentIterator(Iterator):
    """Iterator that runs an operation at each iteration.

    Attributes:
        deployment_log: DeploymentLog object to update.
    """

    def __init__(
        self,
        deployment_log: DeploymentLog,
        operations: List[Operation],
        run_method: Callable[[Operation], OperationLog],
        cluster_variables: ClusterVariables,
    ):
        """Initialize the iterator.

        Args:
            deployment_log: DeploymentLog object to update.
            operations: List of operations to run.
            run_method: Method to run the operation.
            cluster_variables: ClusterVariables object.
        """
        self.deployment_log = deployment_log
        self.deployment_log.start_time = datetime.utcnow()
        self._run_operation = run_method
        self._cluster_variables = cluster_variables
        operations_with_operation_logs: List[Tuple[Operation, OperationLog]] = [
            (operations[i], self.deployment_log.operations[i])
            for i in range(len(operations))
        ]
        self._iter = iter(operations_with_operation_logs)
        self._component_states = defaultdict(_Flags)

    def __next__(self) -> Tuple[OperationLog, ServiceComponentLog]:
        try:
            while True:
                (operation, operation_log) = next(self._iter)

                if self.deployment_log.state == DeploymentStateEnum.FAILURE:
                    operation_log.state = OperationStateEnum.HELD
                    return operation_log, None

                # Retrieve the component state.
                # This is a reference to the object, so we can update it.
                component_state = self._component_states[
                    (operation.service_name, operation.component_name)
                ]
                if operation.action_name == "config":
                    component_state.is_configured = True
                if (
                    operation.action_name in ("start", "restart")
                    and component_state.is_configured == True
                    and component_state.is_started == False
                ):
                    service_component_log = ServiceComponentLog(
                        service=operation.service_name,
                        component=operation.component_name,
                        version=self._cluster_variables[operation.service_name].version,
                    )
                    service_component_log.deployment = self.deployment_log
                    component_state.is_started = True
                else:
                    service_component_log = None

                if operation.noop == False:
                    result = self._run_operation(operation)
                    operation_log.state = result.state
                    operation_log.end_time = result.end_time
                    operation_log.start_time = result.start_time
                    operation_log.logs = result.logs
                    operation_log.state = result.state
                    if operation_log.state != OperationStateEnum.SUCCESS:
                        self.deployment_log.state = DeploymentStateEnum.FAILURE
                else:
                    operation_log.state = OperationStateEnum.SUCCESS

                return operation_log, service_component_log
        # StopIteration is a "normal" exception raised when the iteration has stopped
        except StopIteration as e:
            self.deployment_log.end_time = datetime.utcnow()
            if not self.deployment_log.state == DeploymentStateEnum.FAILURE:
                self.deployment_log.state = DeploymentStateEnum.SUCCESS
            raise e
        # An unforeseen error has occured, stop the deployment and set as failure
        except Exception as e:
            self.deployment_log.end_time = datetime.utcnow()
            self.deployment_log.state = DeploymentStateEnum.FAILURE
            raise e
