# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Iterator
from datetime import datetime
from operator import itemgetter

from tdp.core.models.deployment_log import DeploymentLog, FilterTypeEnum
from tdp.core.models.operation_log import OperationLog
from tdp.core.models.service_log import ServiceLog
from tdp.core.operation import Operation
from tdp.core.runner.executor import StateEnum

logger = logging.getLogger("tdp").getChild("operation_runner")


class OperationIterator(Iterator):
    def __init__(self, dag, service_managers, deployment_log, operations):
        self._dag = dag
        self._service_managers = service_managers
        self._deployment_log = deployment_log
        self._operations = operations

    @property
    def deployment_log(self):
        return self._deployment_log

    def __next__(self):
        try:
            operation_log = next(self._operations)
            operation_log.deployment = self._deployment_log
            return operation_log
        except StopIteration as e:
            self.fill_deployment_log_at_end()
            raise e
        except Exception as e:
            self.fill_deployment_log_at_end(state=StateEnum.FAILURE.value)
            raise e

    def fill_deployment_log_at_end(self, state=None):
        self.deployment_log.end_time = datetime.utcnow()
        self.deployment_log.state = (
            state if state else self.deployment_log.operations[-1].state
        )
        self.deployment_log.services = self._build_service_logs(
            self.deployment_log.operations
        )

    def _services_from_operations(self, operation_names):
        """Returns a set of services from an operation list"""
        return {
            self._dag.collections.operations[operation_name].service
            for operation_name in operation_names
            if not self._dag.collections.operations[operation_name].noop
        }

    def _build_service_logs(self, operation_logs):
        services = self._services_from_operations(
            operation_log.operation for operation_log in operation_logs
        )
        return [
            ServiceLog(
                service=self._service_managers[service_name].name,
                version=self._service_managers[service_name].version,
            )
            for service_name in services
        ]


class EmptyOperationPlan(Exception):
    pass


class OperationPlan:
    def __init__(
        self,
        operations,
        using_dag,
        targets=None,
        sources=None,
        filter_expression=None,
        filter_type=None,
        restart=False,
    ):
        self.operations = operations
        self.targets = targets
        self.sources = sources
        self.filter_expression = filter_expression
        self.filter_type = filter_type
        self.restart = restart
        self.using_dag = using_dag

    def get_deployment_args(self):
        deployment_args = {"using_dag": self.using_dag}
        if self.using_dag:
            deployment_args.update(
                {
                    "targets": self.targets,
                    "sources": self.sources,
                    "filter_expression": self.filter_expression,
                    "filter_type": self.filter_type,
                }
            )
        else:
            deployment_args.update({"targets": self.operations})
        return deployment_args

    @staticmethod
    def from_dag(
        dag, targets=None, sources=None, filter_expression=None, restart=False
    ):
        operation_names = dag.get_operations(sources=sources, targets=targets)
        if filter_expression and hasattr(filter_expression, "search"):
            operation_names = dag.filter_operations_regex(
                operation_names, filter_expression
            )
            str_filter = filter_expression.pattern
            filter_type = FilterTypeEnum.REGEX.value
        elif filter_expression:
            operation_names = dag.filter_operations_glob(
                operation_names, filter_expression
            )
            str_filter = filter_expression
            filter_type = FilterTypeEnum.GLOB.value
        else:
            str_filter = None
            filter_type = None
        # Converting operations names into their actual Operation object
        operations = itemgetter(*operation_names)(dag.operations)
        # Removing all noop operations
        operations = list(filter(lambda operation: operation.noop is False, operations))
        if len(operations) == 0:
            raise EmptyOperationPlan(
                "Combination of parameters resulted into an empty list of Operations (noop excluded)"
            )
        return OperationPlan(
            operations,
            using_dag=True,
            targets=targets,
            sources=sources,
            filter_expression=str_filter,
            filter_type=filter_type,
            restart=restart,
        )

    @staticmethod
    def from_operations(operations, restart=False):
        return OperationPlan(operations, using_dag=False, restart=restart)


class OperationRunner:
    """Run operations"""

    def __init__(self, dag, executor, service_managers):
        self.dag = dag
        self._executor = executor
        self._service_managers = service_managers

    def run(self, operation, operation_file):
        logger.debug(f"Running operation {operation}")

        start = datetime.utcnow()
        state, logs = self._executor.execute(operation_file)
        end = datetime.utcnow()

        if not StateEnum.has_value(state):
            logger.error(
                f"Invalid state ({state}) returned by {self._executor.__class__.__name__}.run('{operation_file}'))"
            )
            state = StateEnum.FAILURE
        elif not isinstance(state, StateEnum):
            state = StateEnum(state)

        return OperationLog(
            operation=operation,
            start_time=start,
            end_time=end,
            state=state.value,
            logs=logs,
        )

    def _run_operations(self, operations, restart=False):
        for operation in operations:
            if isinstance(operation, str):
                operation = self.dag.collections.operations[operation]
            elif isinstance(operation, Operation):
                pass
            else:
                raise ValueError(
                    f"operation should be of type str or Operation, not {type(operation)}"
                )
            if not operation.noop:
                if restart:
                    operation_name = operation.name.replace("_start", "_restart")
                    operation = self.dag.collections.operations[operation_name]
                operation_file = self.dag.collections[
                    operation.collection_name
                ].operations[operation.name]
                operation_log = self.run(operation.name, operation_file)
                if operation_log.state == StateEnum.FAILURE.value:
                    logger.error(f"Operation {operation.name} failed !")
                    yield operation_log
                    return
                logger.info(f"Operation {operation.name} success")
                yield operation_log

    def run_nodes(
        self, sources=None, targets=None, filter_expression=None, restart=False
    ):
        operation_plan = OperationPlan.from_dag(
            self.dag, targets, sources, filter_expression, restart
        )
        return self.run_operation_plan(operation_plan)

    def run_operations(self, operations, restart=False):
        operation_plan = OperationPlan.from_operations(operations, restart)
        return self.run_operation_plan(operation_plan)

    def run_operation_plan(self, operation_plan):
        start = datetime.utcnow()
        operation_logs_generator = self._run_operations(
            operation_plan.operations, operation_plan.restart
        )

        deployment_log = DeploymentLog(
            start_time=start,
            state=StateEnum.PENDING.value,
            **operation_plan.get_deployment_args(),
        )

        return OperationIterator(
            self.dag, self._service_managers, deployment_log, operation_logs_generator
        )
