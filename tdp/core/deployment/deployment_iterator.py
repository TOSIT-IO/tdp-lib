# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from tdp.core.cluster_status import ClusterStatus
from tdp.core.collections import OPERATION_SLEEP_NAME
from tdp.core.models import (
    DeploymentModel,
    DeploymentStateEnum,
    NothingToReconfigureError,
    OperationLog,
    OperationStateEnum,
    SCHStatusLog,
    SCHStatusLogSourceEnum,
)
from tdp.core.service_component_host_name import ServiceComponentHostName
from tdp.core.service_component_name import ServiceComponentName

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.variables import ClusterVariables

logger = logging.getLogger(__name__)


def _group_hosts_by_operation(
    deployment: DeploymentModel,
) -> Optional[OrderedDict[str, set[str]]]:
    """Group hosts by operation.

    Example:
        >>> _group_hosts_by_operation(DeploymentModel(
        ...     operations=[
        ...         OperationLog(operation="s_c_install", host="host1"),
        ...         OperationLog(operation="s_c_config", host="host1"),
        ...         OperationLog(operation="s_c_config", host="host2"),
        ...     ]
        ... ))
        {'s_c_install': {'host1'}, 's_c_config': {'host1', 'host2'}}
    """
    if not deployment.operations:
        return None

    operation_to_hosts_set = OrderedDict()
    for operation_log in deployment.operations:
        operation_to_hosts_set.setdefault(operation_log.operation, set()).add(
            operation_log.host
        )
    return operation_to_hosts_set


class DeploymentIterator(Iterator[Optional[list[SCHStatusLog]]]):
    """Iterator that runs an operation at each iteration.

    Attributes:
        deployment: DeploymentModel object to mutate.
    """

    def __init__(
        self,
        deployment: DeploymentModel,
        *,
        collections: Collections,
        run_method: Callable[[OperationLog], None],
        cluster_variables: ClusterVariables,
        cluster_status: ClusterStatus,
        force_stale_update: bool,
    ):
        """Initialize the iterator.

        Args:
            deployment: DeploymentModel object to mutate.
            collections: Collections instance.
            run_method: Method to run the operation.
            cluster_variables: ClusterVariables instance.
            cluster_status: ClusterStatus instance.
        """
        self.deployment = deployment
        self.deployment.start_time = datetime.utcnow()
        self._cluster_status = cluster_status
        self._collections = collections
        self._run_operation = run_method
        self._cluster_variables = cluster_variables
        self.force_stale_update = force_stale_update
        self._iter = iter(deployment.operations)
        try:
            self._reconfigure_operations = _group_hosts_by_operation(
                DeploymentModel.from_stale_components(
                    self._collections, self._cluster_status
                )
            )
        except NothingToReconfigureError:
            self._reconfigure_operations = None

    def __next__(self) -> Optional[list[SCHStatusLog]]:
        try:
            while True:
                operation_log: OperationLog = next(self._iter)

                # Return early if deployment failed
                if self.deployment.status == DeploymentStateEnum.FAILURE:
                    operation_log.state = OperationStateEnum.HELD
                    return

                # Retrieve operation to access parsed attributes and playbook
                operation = self._collections.get_operation(operation_log.operation)

                # Run the operation
                if operation.noop:
                    # A noop operation is always successful
                    operation_log.state = OperationStateEnum.SUCCESS
                else:
                    self._run_operation(operation_log)

                # Set deployment status to failure if the operation failed
                if operation_log.state != OperationStateEnum.SUCCESS:
                    self.deployment.status = DeploymentStateEnum.FAILURE
                    # Return early as status is not updated
                    return

                # ===== Update the cluster status if success =====

                # Skip sleep operation
                if operation.name == OPERATION_SLEEP_NAME:
                    return

                sch_status_logs: list[SCHStatusLog] = []
                sc_name = ServiceComponentName(
                    service_name=operation.service_name,
                    component_name=operation.component_name,
                )
                is_sc_stale = self._cluster_status.is_sc_stale(
                    sc_name, sc_hosts=operation.host_names
                )

                if is_sc_stale:
                    # Get the first reconfigure operation if any
                    if self._reconfigure_operations:
                        try:
                            first_reconfigure_operation = next(
                                iter(self._reconfigure_operations)
                            )
                        except StopIteration:
                            first_reconfigure_operation = None
                    else:
                        first_reconfigure_operation = None

                    can_update_stale = self.force_stale_update or (
                        operation_log.operation == first_reconfigure_operation
                    )

                    # Log a warning if the operation affect a stale SCH which is not the first reconfigure operation (if any)
                    if not can_update_stale:
                        logger.warning(
                            f"can't update stale {sc_name} with {operation_log.operation}\n"
                            + "first operation is {first_reconfigure_operation}"
                        )
                else:
                    can_update_stale = False

                # fmt: off
                hosts = (
                    [None] if operation.noop  # A noop operation doesn't have any host
                    else [operation_log.host] if operation_log.host  # Only one operation is launched on a single host
                    else operation.host_names  # Host is not specified, hence the operation is launched on all host
                )
                # fmt: on

                # Update the cluster status for each host
                for host in hosts:
                    sch_status_log = self._cluster_status.update_sch(
                        ServiceComponentHostName(sc_name, host),
                        action_name=operation.action_name,
                        version=self._cluster_variables[operation.service_name].version,
                        can_update_stale=can_update_stale,
                    )
                    if sch_status_log:
                        sch_status_log.deployment_id = self.deployment.id
                        sch_status_log.source = (
                            SCHStatusLogSourceEnum.FORCED
                            if self.force_stale_update
                            else SCHStatusLogSourceEnum.DEPLOYMENT
                        )
                        sch_status_logs.append(sch_status_log)

                # Update the reconfigure_operations dict
                if self._reconfigure_operations:
                    hosts = self._reconfigure_operations.get(
                        operation_log.operation, set()
                    )
                    # If host is defined and needed to be reconfigured,
                    # remove it from the reconfigure_operations dict
                    if operation_log.host and operation_log.host in hosts:
                        hosts.remove(operation_log.host)
                    # If no host is defined, or no host is left,
                    # remove the entire operation from the reconfigure_operations dict
                    if not operation_log.host or len(hosts) == 0:
                        self._reconfigure_operations.pop(operation_log.operation, None)

                return sch_status_logs
        # StopIteration is a "normal" exception raised when the iteration has stopped
        except StopIteration as e:
            self.deployment.end_time = datetime.utcnow()
            if not self.deployment.status == DeploymentStateEnum.FAILURE:
                self.deployment.status = DeploymentStateEnum.SUCCESS
            raise e
        # An unforeseen error has occured, stop the deployment and set as failure
        except Exception as e:
            self.deployment.end_time = datetime.utcnow()
            self.deployment.status = DeploymentStateEnum.FAILURE
            raise e
