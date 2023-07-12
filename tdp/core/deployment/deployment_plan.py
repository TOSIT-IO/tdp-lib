# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import Iterable, List
import logging

from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.models import (
    ComponentVersionLog,
    DeploymentLog,
    DeploymentStateEnum,
    DeploymentTypeEnum,
    FilterTypeEnum,
    OperationLog,
    OperationStateEnum,
)
from tdp.core.operation import Operation
from tdp.core.variables import ClusterVariables


logger = logging.getLogger("tdp").getChild("deployment_plan")


class EmptyDeploymentPlanError(Exception):
    pass


class NothingToResumeError(Exception):
    pass


class NothingToRestartError(Exception):
    pass


class UnsupportedDeploymentTypeError(Exception):
    pass


class NotPlannedDeploymentError(Exception):
    pass


class GeneratedDeploymentPlanMissesOperationError(Exception):
    def __init__(self, message: str, reconstructed_operations: List[str]):
        super(Exception).__init__(message)
        self.reconstructed_operations = reconstructed_operations


class DeploymentPlan:
    """Deployment plan giving the operations to perform.

    Attributes:
        operations: List of operations to perform.
        deployment_log: Deployment information
    """

    def __init__(self, operations: List[Operation], deployment_log: DeploymentLog):
        self._operations = operations
        self.deployment_log = deployment_log
        self.deployment_log.operations = []
        for count, operation in enumerate(self.operations, start=1):
            operation_log = OperationLog(
                state=OperationStateEnum.PLANNED,
                operation=operation.name,
                operation_order=count,
            )
            self.deployment_log.operations.append(operation_log)

    @property
    def operations(self) -> List[Operation]:
        return self._operations

    @staticmethod
    def from_dag(
        dag: Dag,
        targets: List[str] = None,
        sources: List[str] = None,
        filter_expression: str = None,
        filter_type: DeploymentTypeEnum = None,
        restart: bool = False,
    ) -> "DeploymentPlan":
        """Generate a deployment plan from a DAG.

        Args:
            dag: DAG to generate the deployment plan from.
            targets: List of targets to which to deploy.
            sources: List of sources from which to deploy.
            filter_expression: Filter expression to apply on the DAG.
            filter_type: Filter type to apply on the DAG.
            restart: Whether or not to transform start operations to restart.
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
            raise EmptyDeploymentPlanError(
                "Combination of parameters resulted into an empty list of Operations (noop included)"
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

        return DeploymentPlan(operations, deployment_log)

    @staticmethod
    def from_operations(operations: List[Operation]) -> "DeploymentPlan":
        """Generate a deployment plan from a list of operations.

        Args:
            operations: List of operations to perform.
        """
        deployment_log = DeploymentLog(
            targets=[operation.name for operation in operations],
            deployment_type=DeploymentTypeEnum.OPERATIONS,
            state=DeploymentStateEnum.PLANNED,
        )
        return DeploymentPlan(operations, deployment_log)

    @staticmethod
    def from_reconfigure(
        dag: Dag,
        cluster_variables: ClusterVariables,
        deployed_component_version_logs: Iterable[ComponentVersionLog],
    ) -> "DeploymentPlan":
        """Generate a deployment plan based on altered component configuration.

        Args:
            dag: DAG to generate the deployment plan from.
            cluster_variables: Cluster variables.
            deployed_component_version_logs: List of deployed versions.

        Raises:
            RuntimeError: If a service is deployed but the repository is missing.

        Returns:
            Deployment plan.
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
            raise NothingToRestartError()
        deployment_plan = DeploymentPlan.from_dag(
            dag,
            sources=list(operations),
            restart=True,
            filter_expression=r".+_(config|(re|)start)",
            filter_type=FilterTypeEnum.REGEX,
        )
        deployment_plan.deployment_log.deployment_type = DeploymentTypeEnum.RECONFIGURE

        return deployment_plan

    @staticmethod
    def from_failed_deployment(collections: Collections, deployment_log: DeploymentLog):
        """Generate a deployment plan from a failed deployment.

        Args:
            collections: Collections to retrieve the operation from.
            deployment_log: Deployment log.

        Raises:
            NothingToResumeError: If the deployment was successful.
            UnsupportedDeploymentTypeError: If the deployment type is not supported.

        Returns:
            Deployment plan.
        """
        if deployment_log.state != DeploymentStateEnum.FAILURE:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{deployment_log.id} was {deployment_log.state}."
            )

        if not isinstance(deployment_log.deployment_type, DeploymentTypeEnum):
            raise UnsupportedDeploymentTypeError(
                f"Resuming from a {deployment_log.deployment_type} is not supported."
            )

        if len(deployment_log.operations) > 0:
            failed_operation_id = next(
                (
                    i
                    for i, operation in enumerate(deployment_log.operations)
                    if operation.state == OperationStateEnum.FAILURE
                ),
                None,
            )
            operations_names_to_resume = [
                operation.operation
                for operation in deployment_log.operations[failed_operation_id:]
            ]
            operations_to_resume = []
            for operation_name in operations_names_to_resume:
                if operation_name not in collections.operations:
                    forged_operation = Operation(
                        name=operation_name,
                        collection_name="replace_restart_noop",
                        noop=True,
                    )
                    if forged_operation.action_name == "restart":
                        operations_to_resume.append(forged_operation)
                else:
                    operations_to_resume.append(collections.operations[operation_name])
        new_deployment_log = DeploymentLog(
            targets=operations_names_to_resume,
            deployment_type=DeploymentTypeEnum.RESUME,
            state=DeploymentStateEnum.PLANNED,
        )
        return DeploymentPlan(
            operations=operations_to_resume, deployment_log=new_deployment_log
        )

    @staticmethod
    def from_deployment_log(
        collections: Collections, deployment_log: DeploymentLog
    ) -> "DeploymentPlan":
        """Generate a DeploymentPlan from a planned DeploymentLog."""
        if deployment_log.state != DeploymentStateEnum.PLANNED:
            raise NotPlannedDeploymentError(
                f"Cannot generate a deployment plan from a deployment log which is not PLANNED (got {deployment_log.state})."
            )

        operations: List[Operation] = []
        for operation_log in deployment_log.operations:
            operations.append(collections.operations[operation_log.operation])

        return DeploymentPlan(operations, deployment_log)
