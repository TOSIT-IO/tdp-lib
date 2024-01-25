# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from tdp.core.cluster_status import ClusterStatus
from tdp.core.constants import OPERATION_SLEEP_NAME
from tdp.core.models import (
    DeploymentModel,
    DeploymentStateEnum,
    NothingToReconfigureError,
    OperationModel,
    OperationStateEnum,
    SCHStatusLogModel,
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
        ...         OperationModel(operation="s_c_install", host="host1"),
        ...         OperationModel(operation="s_c_config", host="host1"),
        ...         OperationModel(operation="s_c_config", host="host2"),
        ...     ]
        ... ))
        {'s_c_install': {'host1'}, 's_c_config': {'host1', 'host2'}}
    """
    if not deployment.operations:
        return None

    operation_to_hosts_set = OrderedDict()
    for operation in deployment.operations:
        operation_to_hosts_set.setdefault(operation.operation, set()).add(
            operation.host
        )
    return operation_to_hosts_set


class DeploymentIterator(Iterator[Optional[list[SCHStatusLogModel]]]):
    """Iterator that runs an operation at each iteration.

    Attributes:
        deployment: DeploymentModel object to mutate.
    """

    def __init__(
        self,
        deployment: DeploymentModel,
        *,
        collections: Collections,
        run_method: Callable[[OperationModel], None],
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
        # Initialize the deployment state
        self.deployment = deployment
        self.deployment.start_time = datetime.utcnow()
        for operation in deployment.operations:
            operation.state = OperationStateEnum.PENDING
        # Initialize the iterator
        self._cluster_status = cluster_status
        self._collections = collections
        self._run_operation = run_method
        self._cluster_variables = cluster_variables
        self.force_stale_update = force_stale_update
        self._iter = iter(deployment.operations)
        # Initialize the reconfigure_operations dict
        # This dict is used to keep track of the reconfigure operations that are left
        # to run
        try:
            self._reconfigure_operations = _group_hosts_by_operation(
                DeploymentModel.from_stale_components(
                    self._collections, self._cluster_status
                )
            )
        except NothingToReconfigureError:
            self._reconfigure_operations = None

    def __next__(self) -> Optional[list[SCHStatusLogModel]]:
        try:
            while True:
                operation_rec: OperationModel = next(self._iter)

                # Return early if deployment failed
                if self.deployment.state == DeploymentStateEnum.FAILURE:
                    operation_rec.state = OperationStateEnum.HELD
                    return
                else:
                    operation_rec.state = OperationStateEnum.RUNNING

                # Retrieve operation to access parsed attributes and playbook
                operation = self._collections.get_operation(operation_rec.operation)

                # Run the operation
                if operation.noop:
                    # A noop operation is always successful
                    operation_rec.state = OperationStateEnum.SUCCESS
                else:
                    self._run_operation(operation_rec)

                # Set deployment status to failure if the operation failed
                if operation_rec.state != OperationStateEnum.SUCCESS:
                    self.deployment.end_time = datetime.utcnow()
                    self.deployment.state = DeploymentStateEnum.FAILURE
                    # Return early as status is not updated
                    return

                # ===== Update the cluster status if success =====

                # Skip sleep operation
                if operation.name == OPERATION_SLEEP_NAME:
                    return

                sch_status_logs: list[SCHStatusLogModel] = []
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
                        operation_rec.operation == first_reconfigure_operation
                    )

                    # Log a warning if the operation affect a stale SCH which is not the first reconfigure operation (if any)
                    if not can_update_stale:
                        logger.warning(
                            f"can't update stale {sc_name} with {operation_rec.operation}\n"
                            + "first operation is {first_reconfigure_operation}"
                        )
                else:
                    can_update_stale = False

                # fmt: off
                hosts = (
                    [None] if operation.noop  # A noop operation doesn't have any host
                    else [operation_rec.host] if operation_rec.host  # Only one operation is launched on a single host
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
                        operation_rec.operation, set()
                    )
                    # If host is defined and needed to be reconfigured,
                    # remove it from the reconfigure_operations dict
                    if operation_rec.host and operation_rec.host in hosts:
                        hosts.remove(operation_rec.host)
                    # If no host is defined, or no host is left,
                    # remove the entire operation from the reconfigure_operations dict
                    if not operation_rec.host or len(hosts) == 0:
                        self._reconfigure_operations.pop(operation_rec.operation, None)

                return sch_status_logs
        # StopIteration is a "normal" exception raised when the iteration has stopped
        except StopIteration as e:
            self.deployment.end_time = datetime.utcnow()
            if not self.deployment.state == DeploymentStateEnum.FAILURE:
                self.deployment.state = DeploymentStateEnum.SUCCESS
            raise e
        # An unforeseen error has occured, stop the deployment and set as failure
        except Exception as e:
            self.deployment.end_time = datetime.utcnow()
            self.deployment.state = DeploymentStateEnum.FAILURE
            raise e

    def get_ansible_commands(self) -> list[str]:
        command_list = []
        for operationM in self.deployment.operations:
            operation = self._collections.get_operation(operationM.operation)
            try:
                operationM.start_time = datetime.utcnow()
                operation = self._collections.get_operation(operationM.operation)
                playbook = self._collections[operation.collection_name].playbooks[
                    operation.name
                ]
                playbook = playbook
                command = "ansible-playbook"
                command += " " + str(playbook)
                if operationM.host is not None:
                    command += " --limit", operationM.host
                for extra_var in operationM.extra_vars or []:
                    command += f" --extra-vars {extra_var}"
                command_list.append(command)
                operationM.end_time = datetime.utcnow()
                operationM.state = OperationStateEnum.SUCCESS
            except:
                continue
        self.deployment.end_time = datetime.utcnow()
        self.deployment.state = DeploymentStateEnum.SUCCESS
        return command_list
