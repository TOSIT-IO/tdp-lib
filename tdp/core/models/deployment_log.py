# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship
from tabulate import tabulate

from tdp.core.collections import OPERATION_SLEEP_NAME, OPERATION_SLEEP_VARIABLE
from tdp.core.dag import Dag
from tdp.core.models.base import Base
from tdp.core.models.operation_log import OperationLog
from tdp.core.models.state_enum import DeploymentStateEnum, OperationStateEnum
from tdp.core.utils import BaseEnum

if TYPE_CHECKING:
    from tdp.core.collections import Collections
    from tdp.core.models import ComponentVersionLog, StaleComponent

logger = logging.getLogger("tdp").getChild("deployment_log")


class NoOperationMatchError(Exception):
    pass


class NothingToReconfigureError(Exception):
    pass


class NothingToResumeError(Exception):
    pass


class UnsupportedDeploymentTypeError(Exception):
    pass


class DeploymentTypeEnum(BaseEnum):
    DAG = "Dag"
    OPERATIONS = "Operations"
    RESUME = "Resume"
    RECONFIGURE = "Reconfigure"


class FilterTypeEnum(BaseEnum):
    REGEX = "regex"
    GLOB = "glob"


class DeploymentLog(Base):
    """Deployment log model.

    Hold past and current deployment information.
    """

    __tablename__ = "deployment_log"

    id: Mapped[int] = mapped_column(primary_key=True, doc="Deployment log id.")
    options: Mapped[dict] = mapped_column(
        JSON(none_as_null=True), doc="Deployment options."
    )
    start_time: Mapped[Optional[datetime]] = mapped_column(doc="Deployment start time.")
    end_time: Mapped[Optional[datetime]] = mapped_column(doc="Deployment end time.")
    status: Mapped[Optional[DeploymentStateEnum]] = mapped_column(
        doc="Deployment status."
    )
    deployment_type: Mapped[Optional[DeploymentTypeEnum]] = mapped_column(
        doc="Deployment type."
    )

    operations: Mapped[list[OperationLog]] = relationship(
        back_populates="deployment",
        order_by="OperationLog.operation_order",
        cascade="all, delete-orphan",
        doc="List of operations.",
    )
    component_version: Mapped[list[ComponentVersionLog]] = relationship(
        back_populates="deployment",
        doc="List of component versions.",
    )

    def __str__(self):
        return tabulate(
            [
                ["id", self.id],
                ["start_time", self.start_time],
                ["end_time", self.end_time],
                ["state", self.status],
            ],
            tablefmt="plain",
        )

    @staticmethod
    def from_dag(
        dag: Dag,
        targets: Optional[list[str]] = None,
        sources: Optional[list[str]] = None,
        filter_expression: Optional[str] = None,
        filter_type: Optional[FilterTypeEnum] = None,
        restart: bool = False,
    ) -> "DeploymentLog":
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
            sources=sources, targets=targets, restart=restart
        )

        if filter_expression is not None:
            if filter_type == FilterTypeEnum.REGEX:
                operations = dag.filter_operations_regex(operations, filter_expression)
            else:
                operations = dag.filter_operations_glob(operations, filter_expression)
                # default behavior is glob
                filter_type = FilterTypeEnum.GLOB

        if len(operations) == 0:
            raise NoOperationMatchError(
                "Combination of parameters resulted into an empty list of Operations."
            )

        deployment_log = DeploymentLog(
            deployment_type=DeploymentTypeEnum.DAG,
            options={
                "restart": restart,  # ? should we store restart if it's False ?
                **_filter_falsy_options(
                    {
                        "targets": targets,
                        "sources": sources,
                        "filter_expression": filter_expression,
                        "filter_type": filter_type,
                    }
                ),
            },
            status=DeploymentStateEnum.PLANNED,
        )
        deployment_log.operations = [
            OperationLog(
                operation=operation.name,
                operation_order=i,
                host=None,
                extra_vars=None,
                state=OperationStateEnum.PLANNED,
            )
            for i, operation in enumerate(operations, 1)
        ]
        return deployment_log

    @staticmethod
    def from_operations(
        collections: Collections,
        operation_names: list[str],
        host_names: Optional[Iterable[str]] = None,
        extra_vars: Optional[Iterable[str]] = None,
        rolling_interval: Optional[int] = None,
    ) -> "DeploymentLog":
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
        operations = [collections.get_operation(o) for o in operation_names]
        if host_names is not None:
            collections.check_operations_hosts_exist(
                operation_names,
                host_names,
            )
        deployment_log = DeploymentLog(
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
            status=DeploymentStateEnum.PLANNED,
        )
        i = 1
        for operation in operations:
            can_perform_rolling_restart = (
                rolling_interval is not None
                and operation.action_name == "restart"
                and operation.host_names
            )
            for host_name in host_names or (
                # if restart operation with rolling and no host is specified,
                # run on all hosts
                operation.host_names
                if can_perform_rolling_restart
                else [None]
            ):
                deployment_log.operations.append(
                    OperationLog(
                        operation=operation.name,
                        operation_order=i,
                        host=host_name,
                        extra_vars=list(extra_vars) if extra_vars else None,
                        state=OperationStateEnum.PLANNED,
                    )
                )
                if can_perform_rolling_restart:
                    i += 1
                    deployment_log.operations.append(
                        OperationLog(
                            operation=OPERATION_SLEEP_NAME,
                            operation_order=i,
                            host=host_name,
                            extra_vars=[
                                f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"
                            ],
                            state=OperationStateEnum.PLANNED,
                        )
                    )
                i += 1
        return deployment_log

    @staticmethod
    def from_stale_components(
        collections: Collections,
        stale_components: list[StaleComponent],
        rolling_interval: Optional[int] = None,
    ) -> "DeploymentLog":
        """Generate a deployment plan from a list of stale components.

        Args:
            collections: Collections to retrieve the operations from.
            stale_components: List of stale components to perform.
            rolling_interval: Number of seconds to wait between component restart.

        Raises:
            NothingToReconfigureError: If no component needs to be reconfigured.
        """
        operation_host_names: set[tuple[str, str]] = set()
        for stale_component in stale_components:
            # should not append as those components should have been filtered out
            if not stale_component.to_reconfigure and not stale_component.to_restart:
                continue
            if stale_component.component_name:
                base_operation_name = "_".join(
                    [stale_component.service_name, stale_component.component_name]
                )
            else:
                base_operation_name = stale_component.service_name
            if stale_component.to_restart:
                operation_host_names.add(
                    (
                        "_".join([base_operation_name, "start"]),
                        stale_component.host_name,
                    )
                )
            if stale_component.to_reconfigure:
                operation_host_names.add(
                    (
                        "_".join([base_operation_name, "config"]),
                        stale_component.host_name,
                    )
                )
        if len(operation_host_names) == 0:
            raise NothingToReconfigureError("No component needs to be reconfigured.")
        # Sort the result in order to have host in lexicographical order.
        operation_host_names_sorted = sorted(
            operation_host_names, key=lambda x: f"{x[0]}_{x[1]}"
        )
        dag = Dag(collections)
        # Sort operations with DAG topological sort, convert string operation to
        # Operation instance and replace start action with restart action.
        operation_hosts = list(
            map(
                lambda x: (dag.node_to_operation(x[0], restart=True), x[1]),
                dag.topological_sort_key(
                    operation_host_names_sorted, key=lambda x: x[0]
                ),
            )
        )
        deployment_log = DeploymentLog(
            deployment_type=DeploymentTypeEnum.RECONFIGURE,
            options={
                **_filter_falsy_options(
                    {
                        "rolling_interval": rolling_interval,
                    }
                ),
            },
            status=DeploymentStateEnum.PLANNED,
        )
        operation_order = 1
        for operation, host in operation_hosts:
            deployment_log.operations.append(
                OperationLog(
                    operation=operation.name,
                    operation_order=operation_order,
                    host=host,
                    extra_vars=None,
                    state=OperationStateEnum.PLANNED,
                )
            )
            # Add sleep operation after each "restart"
            if rolling_interval is not None and operation.action_name == "restart":
                operation_order += 1
                deployment_log.operations.append(
                    OperationLog(
                        operation=OPERATION_SLEEP_NAME,
                        operation_order=operation_order,
                        host=None,
                        extra_vars=[f"{OPERATION_SLEEP_VARIABLE}={rolling_interval}"],
                        state=OperationStateEnum.PLANNED,
                    )
                )

            operation_order += 1
        return deployment_log

    @staticmethod
    def from_failed_deployment(
        collections: Collections, failed_deployment_log: "DeploymentLog"
    ) -> "DeploymentLog":
        """Generate a deployment plan from a failed deployment.

        Args:
            collections: Collections to retrieve the operation from.
            deployment_log: Deployment log.

        Raises:
            NothingToResumeError: If the deployment was successful.
            UnsupportedDeploymentTypeError: If the deployment type is not supported.
        """
        if failed_deployment_log.status != DeploymentStateEnum.FAILURE:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{failed_deployment_log.id} "
                + f"was {failed_deployment_log.status}."
            )

        if len(failed_deployment_log.operations) == 0:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{failed_deployment_log.id} has no operations."
            )

        failed_operation_id = next(
            (
                i
                for i, operation in enumerate(failed_deployment_log.operations)
                if operation.state == OperationStateEnum.FAILURE
            ),
            None,
        )
        operations_tuple_to_resume = [
            (operation.operation, operation.host, operation.extra_vars)
            for operation in failed_deployment_log.operations[failed_operation_id:]
        ]
        operations_names_to_resume = [i[0] for i in operations_tuple_to_resume]
        collections.check_operations_exist(operations_names_to_resume)
        deployment_log = DeploymentLog(
            deployment_type=DeploymentTypeEnum.RESUME,
            options={
                "from": failed_deployment_log.id,
            },
            status=DeploymentStateEnum.PLANNED,
        )
        deployment_log.operations = [
            OperationLog(
                operation=operation,
                operation_order=i,
                host=host,
                extra_vars=extra_vars,
                state=OperationStateEnum.PLANNED,
            )
            for i, (operation, host, extra_vars) in enumerate(
                operations_tuple_to_resume, 1
            )
        ]
        return deployment_log


def _filter_falsy_options(options: dict) -> dict:
    """Get options without falsy values.

    Args:
        options: Options to filter.

    Returns:
        Filtered options.
    """
    return {k: v for k, v in options.items() if v}
