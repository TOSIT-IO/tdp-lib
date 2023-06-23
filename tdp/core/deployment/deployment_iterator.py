# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime
from typing import Iterator

from tdp.core.models import (
    DeploymentStateEnum,
    OperationStateEnum,
    ServiceComponentLog,
)


class _Flags:
    def __init__(self, configured=False, started=False):
        self.configured = configured
        self.started = started


class DeploymentIterator(Iterator):
    """Iterator that runs an operation at each iteration.

    Attributes:
        log: DeploymentLog object to update.
        operations: List of operations to run.
        run_method: Method to run the operation.
        cluster_variables: ClusterVariables object.
    """

    def __init__(self, log, operations, run_method, cluster_variables):
        """Initialize the iterator.

        Args:
            log: DeploymentLog object to update.
            operations: List of operations to run.
            run_method: Method to run the operation.
            cluster_variables: ClusterVariables object.
        """
        self.deployment_log = log
        self._operations = operations
        self._run_operation = run_method
        self._cluster_variables = cluster_variables
        operations_with_log = [(self._operations[i], self.deployment_log.operations[i]) for i in range(len(self._operations))]
        self._iter = iter(operations_with_log)
        self._failed = False
        self._service_component_logs = defaultdict(_Flags)
        self.deployment_log.start_time = datetime.utcnow()

    def __next__(self):
        try:
            while True:
                if self._failed == True:
                    raise StopIteration()
                (operation, operation_log) = next(self._iter)
                service_component = self._service_component_logs[
                    (operation.service_name, operation.component_name)
                ]

                service_component_log = None
                if operation.action_name == "config":
                    service_component.configured = True

                if (
                    operation.action_name in ("start", "restart")
                    and service_component.configured == True
                    and service_component.started == False
                ):
                    service_component_log = ServiceComponentLog(
                        service=operation.service_name,
                        component=operation.component_name,
                        version=self._cluster_variables[operation.service_name].version,
                    )
                    service_component_log.deployment = self.deployment_log

                    service_component.started = True

                if operation.noop == False:
                    new_operation_log = self._run_operation(operation)
                    operation_log.state = new_operation_log.state
                    operation_log.end_time = new_operation_log.end_time
                    operation_log.start_time = new_operation_log.start_time
                    operation_log.logs = new_operation_log.logs
                    operation_log.state = new_operation_log.state
                    self._failed = operation_log.state == OperationStateEnum.FAILURE
                else:
                    operation_log.state = OperationStateEnum.SUCCESS

                return operation_log, service_component_log
        # StopIteration is a "normal" exception raised when the iteration has stopped
        except StopIteration as e:
            self.deployment_log.end_time = datetime.utcnow()
            if len(self.deployment_log.operations) > 0:
                self.deployment_log.state = self.deployment_log.operations[-1].state
            else:
                # case deployment is finised with only noop performed
                self.deployment_log.state = DeploymentStateEnum.SUCCESS
            raise e
        # An unforeseen error has occured, stop the deployment and set as failure
        except Exception as e:
            self.deployment_log.end_time = datetime.utcnow()
            self.deployment_log.state = DeploymentStateEnum.FAILURE
            raise e
