# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Optional

from tdp.core.models import (
    ComponentVersionLog,
    DeploymentStateEnum,
    OperationLog,
    OperationStateEnum,
    StaleComponent,
)

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.models import DeploymentLog
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


class DeploymentIterator(
    Iterator[
        tuple[OperationLog, Optional[ComponentVersionLog], Optional[StaleComponent]]
    ]
):
    """Iterator that runs an operation at each iteration.

    Attributes:
        deployment_log: DeploymentLog object to update.
    """

    def __init__(
        self,
        deployment_log: DeploymentLog,
        collections: Collections,
        run_method: Callable[[OperationLog], None],
        cluster_variables: ClusterVariables,
        stale_components: list[StaleComponent],
    ):
        """Initialize the iterator.

        Args:
            deployment_log: DeploymentLog object to update.
            operations: List of operations to run.
            run_method: Method to run the operation.
            cluster_variables: ClusterVariables object.
            stale_components: List of stale components to actualize.
        """
        self.deployment_log = deployment_log
        self.deployment_log.start_time = datetime.utcnow()
        self._stale_components = StaleComponent.to_dict(stale_components)
        self._collections = collections
        self._run_operation = run_method
        self._cluster_variables = cluster_variables
        self._iter = iter(deployment_log.operations)
        self._component_states = defaultdict(_Flags)

    def __next__(self):
        try:
            while True:
                operation_log: OperationLog = next(self._iter)

                if self.deployment_log.state == DeploymentStateEnum.FAILURE:
                    operation_log.state = OperationStateEnum.HELD
                    return operation_log, None, None

                operation = self._collections.get_operation(operation_log.operation)
                # TODO: operation component_name should be an empty string instead of None when service operation
                stale_component = self._stale_components.get(
                    (
                        operation.service_name,
                        operation.component_name
                        if not operation.is_service_operation()
                        else "",
                    )
                )
                if operation.action_name == "config":
                    if stale_component:
                        stale_component.to_reconfigure = False
                elif operation.action_name == "restart":
                    if stale_component and not stale_component.to_reconfigure:
                        stale_component.to_restart = False
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
                    component_version_log = ComponentVersionLog(
                        service=operation.service_name,
                        component=operation.component_name,
                        version=self._cluster_variables[operation.service_name].version,
                    )
                    component_version_log.deployment = self.deployment_log
                    component_state.is_started = True
                else:
                    component_version_log = None

                if operation.noop == False:
                    self._run_operation(operation_log)
                    if operation_log.state != OperationStateEnum.SUCCESS:
                        self.deployment_log.state = DeploymentStateEnum.FAILURE
                else:
                    operation_log.state = OperationStateEnum.SUCCESS

                return operation_log, component_version_log, stale_component
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
