# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from typing import List

from tdp.core.dag import Dag
from tdp.core.models import (
    DeploymentLog,
    DeploymentTypeEnum,
    FilterTypeEnum,
    ServiceComponentLog,
    StateEnum,
)
from tdp.core.operation import Operation
from tdp.core.variables import ClusterVariables


class EmptyDeploymentPlanError(Exception):
    pass


class NothingToResumeError(Exception):
    pass


class NothingToRestartError(Exception):
    pass


class UnsupportedDeploymentTypeError(Exception):
    pass


class GeneratedDeploymentPlanMissesOperationError(Exception):
    def __init__(self, message: str, reconstructed_operations: List[str]):
        super(Exception).__init__(message)
        self.reconstructed_operations = reconstructed_operations


class DeploymentPlan:
    """Deployment plan giving the operations to perform.

    Attributes:
        operations: List of operations to perform.
        deployment_args: Deployment arguments.
    """

    def __init__(self, operations: List[Operation], deployment_args: dict):
        self.operations = operations
        self.deployment_args = deployment_args

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

        deployment_args = dict(
            deployment_type=DeploymentTypeEnum.DAG,
            targets=targets,
            sources=sources,
            filter_expression=filter_expression,
            filter_type=filter_type,
            restart=restart,
        )

        return DeploymentPlan(operations, deployment_args)

    @staticmethod
    def from_operations(operations: List[Operation]) -> "DeploymentPlan":
        """Generate a deployment plan from a list of operations.

        Args:
            operations: List of operations to perform.
        """
        deployment_args = dict(
            targets=[operation.name for operation in operations],
            deployment_type=DeploymentTypeEnum.OPERATIONS,
        )
        return DeploymentPlan(operations, deployment_args)

    @staticmethod
    def from_reconfigure(
        dag: Dag,
        cluster_variables: ClusterVariables,
        service_component_deployed_version: ServiceComponentLog,
    ) -> "DeploymentPlan":
        """Generate a deployment plan from a list of service component deployed version.

        Args:
            dag: DAG to generate the deployment plan from.
            cluster_variables: Cluster variables.
            service_component_deployed_version: List of deployed versions.

        Raises:
            RuntimeError: If a service is deployed but the repository is missing.

        Returns:
            Deployment plan.
        """
        components_modified = set()
        for (
            service,
            _component,
            version,
        ) in service_component_deployed_version:
            if service not in cluster_variables:
                raise RuntimeError(
                    f"Service '{service}' is deployed but the repository is missing."
                )
            for component_modified in cluster_variables[service].components_modified(
                dag, version
            ):
                components_modified.add(component_modified.name)

        if len(components_modified) == 0:
            raise NothingToRestartError()
        deployment_plan = DeploymentPlan.from_dag(
            dag,
            sources=list(components_modified),
            restart=True,
            filter_expression=r".+_(config|(re|)start)",
            filter_type=FilterTypeEnum.REGEX,
        )
        deployment_plan.deployment_args[
            "deployment_type"
        ] = DeploymentTypeEnum.RECONFIGURE

        return DeploymentPlan(
            deployment_plan.operations,
            {
                **deployment_plan.deployment_args,
                "deployment_type": DeploymentTypeEnum.RECONFIGURE,
            },
        )

    @staticmethod
    def from_failed_deployment(
        dag: Dag, deployment_log: DeploymentLog
    ) -> "DeploymentPlan":
        """Generate a deployment plan from a failed deployment.

        Args:
            dag: DAG to generate the deployment plan from.
            deployment_log: Deployment log.

        Raises:
            NothingToResumeError: If the deployment was successful.
            UnsupportedDeploymentTypeError: If the deployment type is not supported.

        Returns:
            Deployment plan.
        """
        if deployment_log.state == StateEnum.SUCCESS:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{deployment_log.id} was successful"
            )

        if deployment_log.deployment_type in (
            DeploymentTypeEnum.DAG,
            DeploymentTypeEnum.RECONFIGURE,
        ):
            original_operations = DeploymentPlan.from_dag(
                dag,
                deployment_log.targets,
                deployment_log.sources,
                deployment_log.filter_expression,
                deployment_log.filter_type,
                deployment_log.restart,
            ).operations
            original_operation_names = [
                operation.name for operation in original_operations
            ]
        elif deployment_log.deployment_type in (
            DeploymentTypeEnum.OPERATIONS,
            DeploymentTypeEnum.RESUME,
        ):
            original_operation_names = deployment_log.targets
            original_operations = [
                dag.collections.operations[name] for name in original_operation_names
            ]
        else:
            raise UnsupportedDeploymentTypeError(
                f"Resuming from a {deployment_log.deployment_type} is not supported"
            )

        if len(deployment_log.operations) > 0:
            last_operation_log = deployment_log.operations[-1]
            try:
                index = original_operation_names.index(last_operation_log.operation)
            except ValueError as e:
                raise GeneratedDeploymentPlanMissesOperationError(
                    f"'{last_operation_log}' is not in the reconstructed operation list from database parameters",
                    original_operation_names,
                ) from e
            remaining_operations = original_operations[index:]
            remaining_operation_names = original_operation_names[index:]
        else:
            remaining_operations = original_operations
            remaining_operation_names = original_operation_names

        deployment_args = dict(
            targets=remaining_operation_names,
            deployment_type=DeploymentTypeEnum.RESUME,
        )
        return DeploymentPlan(remaining_operations, deployment_args)
