# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, NamedTuple, Optional

from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from tabulate import tabulate

from tdp.core.constants import OPERATION_SLEEP_NAME, OPERATION_SLEEP_VARIABLE
from tdp.core.dag import Dag
from tdp.core.entities.operation import PlaybookOperation
from tdp.core.filters import FilterFactory
from tdp.core.models.base_model import BaseModel
from tdp.core.models.enums import (
    DeploymentStateEnum,
    DeploymentTypeEnum,
    FilterTypeEnum,
    OperationStateEnum,
)
from tdp.core.models.operation_model import OperationModel

if TYPE_CHECKING:
    from tdp.core.collections.collections import Collections
    from tdp.core.entities.hosted_entity_status import HostedEntityStatus
    from tdp.core.entities.operation import Operation

logger = logging.getLogger(__name__)


class NoOperationMatchError(Exception):
    pass


class NothingToReconfigureError(Exception):
    pass


class NothingToResumeError(Exception):
    pass


class UnsupportedDeploymentTypeError(Exception):
    pass


class MissingOperationError(Exception):
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        super().__init__(f"Operation {operation_name} not found.")


class MissingHostForOperationError(Exception):
    def __init__(self, operation: Operation, host_name: str):
        self.operation = operation
        self.host_name = host_name
        available_hosts = []
        if isinstance(operation, PlaybookOperation):
            available_hosts = operation.playbook.hosts
        super().__init__(
            f"Host {host_name} not found for operation {operation.name}."
            f"Available hosts are {available_hosts}."
        )


class DeploymentModel(BaseModel):
    """Deployment model.

    Hold past and current deployment information.
    """

    __tablename__ = "deployment"

    id: Mapped[int] = mapped_column(primary_key=True, doc="deployment id.")
    options: Mapped[Optional[dict]] = mapped_column(
        JSON(none_as_null=True), doc="Deployment options."
    )
    start_time: Mapped[Optional[datetime]] = mapped_column(doc="Deployment start time.")
    end_time: Mapped[Optional[datetime]] = mapped_column(doc="Deployment end time.")
    state: Mapped[Optional[DeploymentStateEnum]] = mapped_column(
        doc="Deployment state."
    )
    deployment_type: Mapped[Optional[DeploymentTypeEnum]] = mapped_column(
        doc="Deployment type."
    )

    operations: Mapped[list[OperationModel]] = relationship(
        back_populates="deployment",
        order_by="OperationModel.operation_order",
        cascade="all, delete-orphan",
        doc="List of operations.",
    )

    def __str__(self):
        return tabulate(
            [
                ["id", self.id],
                ["start_time", self.start_time],
                ["end_time", self.end_time],
                ["state", self.state],
            ],
            tablefmt="plain",
        )

    @staticmethod
    def from_dag(
        dag: Dag,
        targets: Optional[Iterable[str]] = None,
        sources: Optional[Iterable[str]] = None,
        filter_expression: Optional[str] = None,
        filter_type: Optional[FilterTypeEnum] = None,
        restart: bool = False,
        reverse: bool = False,
        stop: bool = False,
        rolling_interval: Optional[int] = None,
    ) -> DeploymentModel:
        """Generate a deployment plan from a DAG.

        Args:
            dag: DAG to generate the deployment plan from.
            targets: List of targets to which to deploy.
            sources: List of sources from which to deploy.
            filter_expression: Filter expression to apply on the DAG.
            filter_type: Filter type to apply on the DAG.
            restart: Whether or not to transform start operations to restart.

        Raises:
            EmptyDeploymentPlanError: If the deployment plan is empty.
        """
        operations = dag.get_operations(
            sources=sources,
            targets=targets,
            restart=restart,
            stop=stop,
        )

        if filter_expression:
            filter_type = filter_type or FilterTypeEnum.GLOB
            filter = FilterFactory.create_filter(
                # Default to glob if no filter type is provided
                filter_type,
                filter_expression,
            )
            operations = filter(operations)

        if len(operations) == 0:
            raise NoOperationMatchError(
                "Combination of parameters resulted into an empty list of Operations."
            )

        if reverse:
            operations = reversed(operations)

        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.DAG,
            options={
                **_filter_falsy_options(
                    {
                        "sources": sources,
                        "targets": targets,
                        "filter_type": filter_type,
                        "filter_expression": filter_expression,
                        "restart": restart,
                        "stop": stop,
                        "reverse": reverse,
                    }
                ),
            },
            state=DeploymentStateEnum.PLANNED,
        )
        operation_order = 1
        for operation in operations:
            can_perform_rolling_restart = (
                rolling_interval is not None
                and isinstance(operation, PlaybookOperation)
                and operation.name.action == "restart"
                and len(operation.playbook.hosts) > 0
            )
            deployment.operations.append(
                OperationModel(
                    operation=operation.name.name,
                    operation_order=operation_order,
                    host=None,
                    extra_vars=None,
                    state=OperationStateEnum.PLANNED,
                )
            )
            operation_order += 1
            if can_perform_rolling_restart:
                deployment.operations.append(
                    OperationModel(
                        operation=OPERATION_SLEEP_NAME,
                        operation_order=operation_order,
                        host=None,
                        extra_vars=[f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"],
                        state=OperationStateEnum.PLANNED,
                    )
                )
                operation_order += 1
        return deployment

    @staticmethod
    def from_operations(
        collections: Collections,
        operation_names: list[str],
        host_names: Optional[Iterable[str]] = None,
        extra_vars: Optional[Iterable[str]] = None,
        rolling_interval: Optional[int] = None,
    ) -> DeploymentModel:
        """Generate a deployment plan from a list of operations.

        Args:
            collections: Collections to retrieve the operations from.
            operations: List of operations names to perform.
            host_names: Set of host for each operation.
            extra_vars: List of extra vars for each operation.
            rolling_interval: Number of seconds to wait between component restart.

        Raises:
            UnsupportedOperationError: If an operation is a noop.
            ValueError: If an operation is not found in the collections.
        """
        operations = [collections.operations[o] for o in operation_names]
        for host in host_names or []:
            for operation in operations:
                if not isinstance(operation, PlaybookOperation) or (
                    host not in operation.playbook.hosts
                ):
                    raise MissingHostForOperationError(operation, host)
        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.OPERATIONS,
            options={
                "operations": operation_names,
                **_filter_falsy_options(
                    {
                        "hosts": host_names,
                        "extra_vars": extra_vars,
                        "rolling_interval": rolling_interval,
                    }
                ),
            },
            state=DeploymentStateEnum.PLANNED,
        )
        operation_order = 1
        for operation in operations:
            can_perform_rolling_restart = (
                rolling_interval is not None
                and isinstance(operation, PlaybookOperation)
                and operation.name.action == "restart"
                and len(operation.playbook.hosts) > 0
            )
            for host_name in host_names or (
                # if restart operation with rolling and no host is specified,
                # run on all hosts
                operation.playbook.hosts  # type: ignore : operation is a PlaybookOperation
                if can_perform_rolling_restart
                else [None]
            ):
                deployment.operations.append(
                    OperationModel(
                        operation=operation.name.name,
                        operation_order=operation_order,
                        host=host_name,
                        extra_vars=list(extra_vars) if extra_vars else None,
                        state=OperationStateEnum.PLANNED,
                    )
                )
                if can_perform_rolling_restart:
                    operation_order += 1
                    deployment.operations.append(
                        OperationModel(
                            operation=OPERATION_SLEEP_NAME,
                            operation_order=operation_order,
                            host=None,
                            extra_vars=[
                                f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"
                            ],
                            state=OperationStateEnum.PLANNED,
                        )
                    )
                operation_order += 1
        return deployment

    @staticmethod
    def from_operations_hosts_vars(
        collections: Collections,
        operation_host_vars_names: list[tuple[str, Optional[str], Optional[list[str]]]],
    ) -> DeploymentModel:
        """Generate a deployment plan from a list of operations, hosts and extra vars.

        Args:
            collections: Collections to retrieve the operations from.
            operation_host_vars_names: List of operations, hosts and extra vars to perform.

        Raises:
            MissingOperationError: If an operation is a noop.
            MissingHostForOperationError: If an operation is not found in the collections.
        """
        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.CUSTOM,
            state=DeploymentStateEnum.PLANNED,
        )

        for operation_order, operation_host_vars in enumerate(
            operation_host_vars_names, start=1
        ):
            operation_name, host_name, var_names = operation_host_vars
            operation = collections.operations[operation_name]
            if host_name and (
                not isinstance(operation, PlaybookOperation)
                or host_name not in operation.playbook.hosts
            ):
                raise MissingHostForOperationError(operation, host_name)
            else:
                if operation_name not in collections.operations:
                    raise MissingOperationError(operation_name)

            deployment.operations.append(
                OperationModel(
                    operation=operation_name,
                    operation_order=operation_order,
                    host=host_name,
                    extra_vars=var_names,
                    state=OperationStateEnum.PLANNED,
                )
            )
        return deployment

    @staticmethod
    def from_stale_hosted_entities(
        collections: Collections,
        stale_hosted_entity_statuses: list[HostedEntityStatus],
        rolling_interval: Optional[int] = None,
    ) -> DeploymentModel:
        """Generate a deployment plan for stale components.

        Args:
            collections: Collections to retrieve the operations from.
            stale_hosted_entity_statuses: List of stale hosted entity statuses.
            rolling_interval: Number of seconds to wait between component restart.

        Raises:
            NothingToReconfigureError: If no component needs to be reconfigured.
        """
        operation_hosts = _get_reconfigure_operation_hosts(stale_hosted_entity_statuses)

        # Sort operations using DAG topological sort. Convert operation name to
        # Operation instance and replace "start" action by "restart".
        dag = Dag(collections)
        reconfigure_operations_sorted = list(
            map(
                lambda x: (
                    dag.node_to_operation(x.operation_name, restart=True),
                    x.host_name,
                ),
                dag.topological_sort_key(
                    operation_hosts, key=lambda x: x.operation_name
                ),
            )
        )

        # Generate deployment
        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.RECONFIGURE,
            options={
                **_filter_falsy_options(
                    {
                        "rolling_interval": rolling_interval,
                    }
                ),
            },
            state=DeploymentStateEnum.PLANNED,
        )
        operation_order = 1
        for operation, host in reconfigure_operations_sorted:
            deployment.operations.append(
                OperationModel(
                    operation=operation.name.name,
                    operation_order=operation_order,
                    host=host,
                    extra_vars=None,
                    state=OperationStateEnum.PLANNED,
                )
            )
            # Add sleep operation after each "restart"
            if rolling_interval is not None and operation.name.action == "restart":
                operation_order += 1
                deployment.operations.append(
                    OperationModel(
                        operation=OPERATION_SLEEP_NAME,
                        operation_order=operation_order,
                        host=None,
                        extra_vars=[f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"],
                        state=OperationStateEnum.PLANNED,
                    )
                )

            operation_order += 1
        return deployment

    @staticmethod
    def from_failed_deployment(
        collections: Collections, failed_deployment: "DeploymentModel"
    ) -> DeploymentModel:
        """Generate a deployment plan from a failed deployment.

        Args:
            collections: Collections to retrieve the operation from.
            deployment: deployment.

        Raises:
            NothingToResumeError: If the deployment was successful.
            UnsupportedDeploymentTypeError: If the deployment type is not supported.
        """
        if failed_deployment.state != DeploymentStateEnum.FAILURE:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{failed_deployment.id} "
                + f"was {failed_deployment.state}."
            )

        if len(failed_deployment.operations) == 0:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{failed_deployment.id} has no operations."
            )

        failed_operation_id = next(
            (
                operation_order
                for operation_order, operation in enumerate(
                    failed_deployment.operations
                )
                if operation.state == OperationStateEnum.FAILURE
            ),
            None,
        )
        operations_tuple_to_resume = [
            (operation.operation, operation.host, operation.extra_vars)
            for operation in failed_deployment.operations[failed_operation_id:]
        ]
        operations_names_to_resume = [i[0] for i in operations_tuple_to_resume]
        for operation_name_to_resume in operations_names_to_resume:
            if operation_name_to_resume not in collections.operations:
                raise MissingOperationError(operation_name_to_resume)
        deployment = DeploymentModel(
            deployment_type=DeploymentTypeEnum.RESUME,
            options={
                "from": failed_deployment.id,
            },
            state=DeploymentStateEnum.PLANNED,
        )
        deployment.operations = [
            OperationModel(
                operation=operation,
                operation_order=operation_order,
                host=host,
                extra_vars=extra_vars,
                state=OperationStateEnum.PLANNED,
            )
            for operation_order, (operation, host, extra_vars) in enumerate(
                operations_tuple_to_resume, 1
            )
        ]
        return deployment


def _filter_falsy_options(options: dict) -> dict:
    """Get options without falsy values.

    Args:
        options: Options to filter.

    Returns:
        Filtered options.
    """
    return {k: v for k, v in options.items() if v}


class OperationHostTuple(NamedTuple):
    operation_name: str
    host_name: Optional[str]


def _get_reconfigure_operation_hosts(
    stale_hosted_entity_statuses: list[HostedEntityStatus],
) -> list[OperationHostTuple]:
    """Get the list of reconfigure operations from a list of hosted entities statuses.

    Args:
        stale_hosted_entity_statuses: List of stale hosted entities statuses.

    Returns: List of tuple (operation, host) ordered <operation-name>_<host>.
    """
    operation_hosts: set[OperationHostTuple] = set()
    for status in stale_hosted_entity_statuses:
        if status.to_config:
            operation_hosts.add(
                OperationHostTuple(
                    f"{status.entity.name}_config",
                    status.entity.host,
                )
            )
        if status.to_restart:
            operation_hosts.add(
                OperationHostTuple(
                    f"{status.entity.name}_start",
                    status.entity.host,
                )
            )
    if len(operation_hosts) == 0:
        raise NothingToReconfigureError("No component needs to be reconfigured.")
    # Sort by hosts to improve readability
    return sorted(
        operation_hosts,
        key=lambda x: f"{x.operation_name}_{x.host_name}",
    )
