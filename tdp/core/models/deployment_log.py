# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import enum
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Iterable, Optional

from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column, relationship
from tabulate import tabulate

from tdp.core.dag import Dag
from tdp.core.models.base import Base
from tdp.core.models.operation_log import OperationLog
from tdp.core.models.state_enum import DeploymentStateEnum, OperationStateEnum
from tdp.core.operation import OPERATION_NAME_MAX_LENGTH

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


class DeploymentTypeEnum(str, enum.Enum):
    """Deployment type enum.

    Attributes:
        DAG: Dag deployment type.
        OPERATIONS: Operations deployment type.
        RESUME: Resume deployment type.
        RECONFIGURE: Reconfigure deployment type.
    """

    DAG = "Dag"
    OPERATIONS = "Operations"
    RESUME = "Resume"
    RECONFIGURE = "Reconfigure"


class FilterTypeEnum(str, enum.Enum):
    """Filter type enum.

    Attributes:
        REGEX: Regex filter type.
        GLOB: Glob filter type.
    """

    REGEX = "regex"
    GLOB = "glob"


class DeploymentLog(Base):
    """Deployment log model.

    Hold past and current deployment information.

    Attributes:
        id: Deployment log id.
        sources: List of source nodes, in the case of Dag deployment type.
        targets: List of target nodes, in the case of Dag deployment type.
          List of operations, in the case of Run deployment type.
        filter_expression: Filter expression.
        filter_type: Filter type.
        hosts: List of hosts.
        start_time: Deployment start time.
        end_time: Deployment end time.
        state: Deployment state.
        deployment_type: Deployment type.
        restart: Restart flag.
    """

    __tablename__ = "deployment_log"

    id: Mapped[int] = mapped_column(primary_key=True)
    sources: Mapped[Optional[list[str]]] = mapped_column(JSON(none_as_null=True))
    targets: Mapped[Optional[list[str]]] = mapped_column(JSON(none_as_null=True))
    filter_expression: Mapped[Optional[str]] = mapped_column(
        String(OPERATION_NAME_MAX_LENGTH * 5)
    )
    filter_type: Mapped[Optional[FilterTypeEnum]]
    hosts: Mapped[Optional[list[str]]] = mapped_column(JSON(none_as_null=True))
    start_time: Mapped[Optional[datetime]]
    end_time: Mapped[Optional[datetime]]
    state: Mapped[Optional[DeploymentStateEnum]]
    deployment_type: Mapped[Optional[DeploymentTypeEnum]]
    restart: Mapped[Optional[bool]] = mapped_column(default=False)

    operations: Mapped[list[OperationLog]] = relationship(
        back_populates="deployment",
        order_by="OperationLog.operation_order",
        cascade="all, delete-orphan",
    )
    component_version: Mapped[list[ComponentVersionLog]] = relationship(
        back_populates="deployment"
    )

    def __str__(self):
        return tabulate(
            [
                ["id", self.id],
                ["sources", self.sources],
                ["targets", self.targets],
                ["filter_expression", self.filter_expression],
                ["filter_type", self.filter_type],
                ["start_time", self.start_time],
                ["end_time", self.end_time],
                ["state", self.state],
                ["deployment_type", self.deployment_type],
                ["restart", self.restart],
            ],
            tablefmt="plain",
        )

    @staticmethod
    def from_dag(
        dag: Dag,
        targets: list[str] = None,
        sources: list[str] = None,
        filter_expression: str = None,
        filter_type: DeploymentTypeEnum = None,
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
                "Combination of parameters resulted into an empty list of Operations (noop included)."
            )

        deployment_log = DeploymentLog(
            deployment_type=DeploymentTypeEnum.DAG,
            state=DeploymentStateEnum.PLANNED,
            targets=targets,
            sources=sources,
            filter_expression=filter_expression,
            filter_type=filter_type,
            restart=restart,
        )
        deployment_log.operations = [
            OperationLog(
                operation=operation.name,
                operation_order=i,
                host=None,
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
    ) -> "DeploymentLog":
        """Generate a deployment plan from a list of operations.

        Args:
            collections: Collections to retrieve the operations from.
            operations: List of operations names to perform.
            host_names: Set of host for each operation.

        Raises:
            UnsupportedOperationError: If an operation is a noop.
            ValueError: If an operation is not found in the collections.
        """
        collections.check_operations_exist(operation_names)
        if host_names is not None:
            collections.check_operations_hosts_exist(
                operation_names,
                host_names,
            )
        deployment_log = DeploymentLog(
            targets=operation_names,
            hosts=list(host_names) if host_names is not None else None,
            deployment_type=DeploymentTypeEnum.OPERATIONS,
            state=DeploymentStateEnum.PLANNED,
        )
        i = 1
        for operation in operation_names:
            for host_name in host_names or [None]:
                deployment_log.operations.append(
                    OperationLog(
                        operation=operation,
                        operation_order=i,
                        host=host_name,
                        state=OperationStateEnum.PLANNED,
                    )
                )
                i += 1
        return deployment_log

    @staticmethod
    def from_stale_components(
        collections: Collections, stale_components: list[StaleComponent]
    ) -> "DeploymentLog":
        """Generate a deployment plan from a list of stale components.

        Args:
            collections: Collections to retrieve the operations from.
            stale_components: List of stale components to perform.

        Raises:
            NothingToReconfigureError: If no component needs to be reconfigured.
        """
        operations_names = set()
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
                operations_names.add("_".join([base_operation_name, "start"]))
            if stale_component.to_reconfigure:
                operations_names.add("_".join([base_operation_name, "config"]))
        if len(operations_names) == 0:
            raise NothingToReconfigureError("No component needs to be reconfigured.")
        dag = Dag(collections)
        operations = dag.topological_sort(nodes=operations_names, restart=True)
        deployment_log = DeploymentLog(
            targets=list([operation.name for operation in operations]),
            deployment_type=DeploymentTypeEnum.RECONFIGURE,
            state=DeploymentStateEnum.PLANNED,
        )
        deployment_log.operations = [
            OperationLog(
                operation=operation.name,
                operation_order=i,
                state=OperationStateEnum.PLANNED,
            )
            for i, operation in enumerate(operations, 1)
        ]
        return deployment_log

    @staticmethod
    def from_failed_deployment(
        collections: Collections, deployment_log: "DeploymentLog"
    ) -> "DeploymentLog":
        """Generate a deployment plan from a failed deployment.

        Args:
            collections: Collections to retrieve the operation from.
            deployment_log: Deployment log.

        Raises:
            NothingToResumeError: If the deployment was successful.
            UnsupportedDeploymentTypeError: If the deployment type is not supported.
        """
        if deployment_log.state != DeploymentStateEnum.FAILURE:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{deployment_log.id} was {deployment_log.state}."
            )

        if not isinstance(deployment_log.deployment_type, DeploymentTypeEnum):
            raise UnsupportedDeploymentTypeError(
                f"Resuming from a {deployment_log.deployment_type} is not supported."
            )

        if len(deployment_log.operations) == 0:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{deployment_log.id} has no operations."
            )

        failed_operation_id = next(
            (
                i
                for i, operation in enumerate(deployment_log.operations)
                if operation.state == OperationStateEnum.FAILURE
            ),
            None,
        )
        operations_names_hosts_to_resume = [
            (operation.operation, operation.host)
            for operation in deployment_log.operations[failed_operation_id:]
        ]
        operations_names_to_resume = [i[0] for i in operations_names_hosts_to_resume]
        collections.check_operations_exist(operations_names_to_resume)
        deployment_log = DeploymentLog(
            targets=operations_names_to_resume,
            deployment_type=DeploymentTypeEnum.RESUME,
            state=DeploymentStateEnum.PLANNED,
        )
        deployment_log.operations = [
            OperationLog(
                operation=operation,
                operation_order=i,
                host=host,
                state=OperationStateEnum.PLANNED,
            )
            for i, (operation, host) in enumerate(operations_names_hosts_to_resume, 1)
        ]
        return deployment_log
