# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Optional

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
    Iterator[tuple[Optional[list[ComponentVersionLog]], Optional[list[StaleComponent]]]]
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

                if self.deployment_log.status == DeploymentStateEnum.FAILURE:
                    operation_log.state = OperationStateEnum.HELD
                    return None, None

                operation = self._collections.get_operation(operation_log.operation)

                # Get the component state if stale
                stale_components: list[StaleComponent] = []
                if operation_log.host or operation.noop:
                    # Host is in the operation log or operation is noop (hence no host),
                    # so we get a single stale_component
                    _stale_component = self._stale_components.get(
                        (
                            operation.service_name,
                            operation.component_name or "",
                            operation_log.host or "",
                        )
                    )
                    if _stale_component:
                        stale_components.append(_stale_component)
                else:
                    # Host is not in the operation log,
                    # so we get stale_components for all hosts
                    for stale_component in self._stale_components.values():
                        if (
                            stale_component.service_name == operation.service_name
                            and stale_component.component_name
                            == operation.component_name
                        ):
                            stale_components.append(stale_component)
                    stale_components.extend(
                        filter(
                            lambda x: x.service_name == operation.service_name
                            and x.component_name == operation.component_name,
                            self._stale_components.values(),
                        )
                    )

                if any(stale_components):
                    # Update the component state
                    if operation.action_name == "config":
                        for stale_component in stale_components:
                            stale_component.to_reconfigure = False
                    elif operation.action_name == "restart":
                        for stale_component in stale_components:
                            # Component must be configured for a restart to be effective
                            if not stale_component.to_reconfigure:
                                stale_component.to_restart = False

                # TODO: move the component_version_log logic along with the stale component
                # For now, it is still needed for the first deployment (no stale component)
                component_version_logs: list[ComponentVersionLog] = []
                component_state = self._component_states[
                    (
                        operation.service_name,
                        operation.component_name,
                        operation_log.host,
                    )
                ]
                if operation.action_name == "config":
                    component_state.is_configured = True
                if (
                    operation.action_name in ("start", "restart")
                    and component_state.is_configured == True
                    and component_state.is_started == False
                ):
                    component_state.is_started = True
                    # Component version that are both configured and started are saved
                    if operation.noop:
                        component_version_log = ComponentVersionLog(
                            service=operation.service_name,
                            component=operation.component_name,
                            version=self._cluster_variables[
                                operation.service_name
                            ].version,
                        )
                        component_version_log.deployment = self.deployment_log
                        component_version_logs.append(component_version_log)
                    elif operation_log.host:
                        component_version_log = ComponentVersionLog(
                            service=operation.service_name,
                            component=operation.component_name,
                            host=operation_log.host,
                            version=self._cluster_variables[
                                operation.service_name
                            ].version,
                        )
                        component_version_log.deployment = self.deployment_log
                        component_version_logs.append(component_version_log)
                    else:
                        for host_name in operation.host_names:
                            component_version_log = ComponentVersionLog(
                                service=operation.service_name,
                                component=operation.component_name,
                                host=host_name,
                                version=self._cluster_variables[
                                    operation.service_name
                                ].version,
                            )
                            component_version_log.deployment = self.deployment_log
                            component_version_logs.append(component_version_log)

                if operation.noop == False:
                    self._run_operation(operation_log)
                    if operation_log.state != OperationStateEnum.SUCCESS:
                        self.deployment_log.status = DeploymentStateEnum.FAILURE
                else:
                    operation_log.state = OperationStateEnum.SUCCESS

                return component_version_logs, stale_components
        # StopIteration is a "normal" exception raised when the iteration has stopped
        except StopIteration as e:
            self.deployment_log.end_time = datetime.utcnow()
            if not self.deployment_log.status == DeploymentStateEnum.FAILURE:
                self.deployment_log.status = DeploymentStateEnum.SUCCESS
            raise e
        # An unforeseen error has occured, stop the deployment and set as failure
        except Exception as e:
            self.deployment_log.end_time = datetime.utcnow()
            self.deployment_log.status = DeploymentStateEnum.FAILURE
            raise e
