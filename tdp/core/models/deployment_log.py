# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import enum
import logging
from typing import Iterable, List, Optional

from sqlalchemy import JSON, Boolean, Column, DateTime, Enum, Integer, String
from sqlalchemy.orm import relationship
from tabulate import tabulate

from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.operation import OPERATION_NAME_MAX_LENGTH
from tdp.core.variables import ClusterVariables

from .base import Base
from .component_version_log import ComponentVersionLog
from .operation_log import OperationLog
from .state_enum import DeploymentStateEnum, OperationStateEnum

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
        id (int): Deployment log id.
        sources (List[str]): List of source nodes, in the case of Dag deployment type.
        targets (List[str]): List of target nodes, in the case of Dag deployment type. List of operations, in the case of Run deployment type.
        filter_expression (str): Filter expression.
        filter_type (FilterTypeEnum): Filter type.
        hosts (List[str]): List of hosts.
        start_time (datetime): Deployment start time.
        end_time (datetime): Deployment end time.
        state (DeploymentStateEnum): Deployment state.
        deployment_type (DeploymentTypeEnum): Deployment type.
        restart (bool): Restart flag.
    """

    __tablename__ = "deployment_log"

    id = Column(Integer, primary_key=True)
    sources = Column(JSON(none_as_null=True))
    targets = Column(JSON(none_as_null=True))
    filter_expression = Column(String(length=OPERATION_NAME_MAX_LENGTH * 5))
    filter_type = Column(Enum(FilterTypeEnum))
    hosts = Column(JSON(none_as_null=True))
    start_time = Column(DateTime(timezone=False))
    end_time = Column(DateTime(timezone=False))
    state: Column = Column(Enum(DeploymentStateEnum))
    deployment_type: Column = Column(Enum(DeploymentTypeEnum))
    restart = Column(Boolean, default=False)

    operations = relationship(
        "OperationLog",
        back_populates="deployment",
        order_by="OperationLog.operation_order",
        cascade="all, delete-orphan",
    )
    component_version = relationship(
        "ComponentVersionLog", back_populates="deployment", cascade_backrefs=False
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
        targets: List[str] = None,
        sources: List[str] = None,
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
        operation_names: List[str],
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
    def from_reconfigure(
        dag: Dag,
        cluster_variables: ClusterVariables,
        deployed_component_version_logs: Iterable[ComponentVersionLog],
    ) -> "DeploymentLog":
        """Generate a deployment plan based on altered component configuration.

        Args:
            dag: DAG to generate the deployment plan from.
            cluster_variables: Cluster variables.
            deployed_component_version_logs: List of deployed versions.

        Raises:
            RuntimeError: If a service is deployed but the repository is missing.
        """
        operations = set()
        modified_components = cluster_variables.get_modified_components_names(
            deployed_component_version_logs,
        )
        for modified_component in modified_components:
            logger.debug(
                f"Component {modified_component.component_name} of service {modified_component.service_name} has been modified"
            )
            if modified_component.is_service:
                service_operations = dag.services_operations[
                    modified_component.service_name
                ]
                # TODO: use dag.filter_operations_glob instead
                service_operations_config = filter(
                    lambda operation: operation.action_name == "config",
                    service_operations,
                )
                operations.update(
                    map(lambda operation: operation.name, service_operations_config)
                )
            else:
                operations.add(f"{modified_component.full_name}_config")
        if len(operations) == 0:
            raise NothingToReconfigureError("No component to reconfigure.")
        deployment_log = DeploymentLog.from_dag(
            dag,
            sources=list(operations),
            restart=True,
            filter_expression=r".+_(config|(re|)start)",
            filter_type=FilterTypeEnum.REGEX,
        )
        deployment_log.deployment_type = DeploymentTypeEnum.RECONFIGURE
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
