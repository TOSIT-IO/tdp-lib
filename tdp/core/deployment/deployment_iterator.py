# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections import OrderedDict
from collections.abc import Callable, Iterator
from datetime import datetime
from functools import partial
from typing import TYPE_CHECKING, Optional

from tdp.core.constants import OPERATION_SLEEP_NAME
from tdp.core.entities.hostable_entity_name import create_hostable_entity_name
from tdp.core.entities.hosted_entity import create_hosted_entity
from tdp.core.models import (
    DeploymentModel,
    NothingToReconfigureError,
    OperationModel,
    SCHStatusLogModel,
    SCHStatusLogSourceEnum,
)
from tdp.core.models.enums import DeploymentStateEnum, OperationStateEnum

if TYPE_CHECKING:
    from tdp.core.cluster_status import ClusterStatus
    from tdp.core.collections import Collections
    from tdp.core.variables import ClusterVariables

logger = logging.getLogger(__name__)

ProcessOperationFn = Callable[[], Optional[list[SCHStatusLogModel]]]


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


class DeploymentIterator(Iterator[tuple[OperationModel, Optional[ProcessOperationFn]]]):
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
                DeploymentModel.from_stale_hosted_entities(
                    collections=self._collections,
                    stale_hosted_entity_statuses=[
                        status for status in cluster_status.values() if status.is_stale
                    ],
                )
            )
        except NothingToReconfigureError:
            self._reconfigure_operations = None

    def __next__(
        self,
    ) -> tuple[OperationModel, Optional[ProcessOperationFn]]:
        try:
            while True:
                operation_rec = next(self._iter)

                # Return early if deployment failed
                if self.deployment.state == DeploymentStateEnum.FAILURE:
                    operation_rec.state = OperationStateEnum.HELD
                    return operation_rec, None

                operation_rec.state = OperationStateEnum.RUNNING

                return operation_rec, partial(self._process_operation_fn, operation_rec)
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

    def _process_operation_fn(
        self, operation_rec: OperationModel
    ) -> Optional[list[SCHStatusLogModel]]:

        operation = self._collections.operations[operation_rec.operation]

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
        entity_name = create_hostable_entity_name(
            operation.service_name, operation.component_name
        )

        if self._cluster_status.is_sc_stale(entity_name, hosts=operation.host_names):
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
                    f"can't update stale {entity_name} with {operation_rec.operation}, the first operation is {first_reconfigure_operation}"
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
            sch_status_log = self._cluster_status.update_hosted_entity(
                create_hosted_entity(entity_name, host),
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
            hosts = self._reconfigure_operations.get(operation_rec.operation, set())
            # If host is defined and needed to be reconfigured,
            # remove it from the reconfigure_operations dict
            if operation_rec.host and operation_rec.host in hosts:
                hosts.remove(operation_rec.host)
            # If no host is defined, or no host is left,
            # remove the entire operation from the reconfigure_operations dict
            if not operation_rec.host or len(hosts) == 0:
                self._reconfigure_operations.pop(operation_rec.operation, None)

        return sch_status_logs
