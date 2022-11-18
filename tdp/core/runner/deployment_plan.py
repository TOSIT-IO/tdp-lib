# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.core.models import FilterTypeEnum
from tdp.core.models.deployment_type_enum import DeploymentTypeEnum
from tdp.core.models.state_enum import StateEnum


class EmptyDeploymentPlanError(Exception):
    pass


class NothingToResumeError(Exception):
    pass


class DeploymentPlan:
    def __init__(self, operations, deployment_args):
        self.operations = operations
        self.deployment_args = deployment_args

    @staticmethod
    def from_dag(
        dag, targets=None, sources=None, filter_expression=None, restart=False
    ):
        operations = dag.get_operations(
            sources=sources, targets=targets, restart=restart
        )
        if filter_expression and hasattr(filter_expression, "search"):
            operations = dag.filter_operations_regex(operations, filter_expression)
            str_filter = filter_expression.pattern
            filter_type = FilterTypeEnum.REGEX
        elif filter_expression:
            operations = dag.filter_operations_glob(operations, filter_expression)
            str_filter = filter_expression
            filter_type = FilterTypeEnum.GLOB
        else:
            str_filter = None
            filter_type = None
        if len(operations) == 0:
            raise EmptyDeploymentPlanError(
                "Combination of parameters resulted into an empty list of Operations (noop included)"
            )

        deployment_args = dict(
            deployment_type=DeploymentTypeEnum.DAG,
            targets=targets,
            sources=sources,
            filter_expression=str_filter,
            filter_type=filter_type,
            restart=restart,
        )

        return DeploymentPlan(operations, deployment_args)

    @staticmethod
    def from_operations(operations):
        deployment_args = dict(
            targets=[operation.name for operation in operations],
            deployment_type=DeploymentTypeEnum.OPERATIONS,
        )
        return DeploymentPlan(operations, deployment_args)

    @staticmethod
    def from_failed_deployment(dag, deployment_log_to_resume):

        if deployment_log_to_resume.deployment_type == DeploymentTypeEnum.DAG:
            deployment_plan_to_resume = DeploymentPlan.from_dag(
                dag,
                deployment_log_to_resume.targets,
                deployment_log_to_resume.sources,
                deployment_log_to_resume.filter_expression,
            )
        elif deployment_log_to_resume.deployment_type == DeploymentTypeEnum.OPERATIONS:
            raise Exception(
                f"Resuming from an Operations deployment is not yet supported"
            )
        elif deployment_log_to_resume.deployment_type == DeploymentTypeEnum.RESUME:
            raise Exception(f"Resuming from an Resume deployment is not yet supported")

        if deployment_log_to_resume.state == StateEnum.SUCCESS:
            raise NothingToResumeError(
                f"Nothing to resume, deployment #{deployment_log_to_resume.id} was successful"
            )

        original_operations = [
            operation.name for operation in deployment_plan_to_resume.operations
        ]
        succeeded_operations = [
            operation.operation
            for operation in deployment_log_to_resume.operations
            if operation.state == StateEnum.SUCCESS
        ]
        remaining_operations_list = list(
            set(original_operations) - set(succeeded_operations)
        )
        remaining_operations = [
            operation
            for operation in deployment_plan_to_resume.operations
            if operation.name in remaining_operations_list
        ]

        deployment_args = dict(
            targets=[operation.name for operation in remaining_operations],
            deployment_type=DeploymentTypeEnum.RESUME,
        )
        return DeploymentPlan(remaining_operations, deployment_args)
