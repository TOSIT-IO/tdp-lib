# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from tdp.core.models import FilterTypeEnum


class EmptyDeploymentPlanError(Exception):
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
            using_dag=True,
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
            using_dag=False,
        )
        return DeploymentPlan(operations, deployment_args)
