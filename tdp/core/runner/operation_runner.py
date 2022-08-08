# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Iterator
from datetime import datetime

from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.models.operation_log import OperationLog
from tdp.core.models.service_log import ServiceLog
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
            self.deployment_log.operations.append(operation_log)
            return operation_log
        except StopIteration as e:
            self.fill_deployment_log_at_end()
            raise e
        except Exception as e:
            self.fill_deployment_log_at_end(state=StateEnum.FAILURE.value)
            raise e

    def fill_deployment_log_at_end(self, state=None):
        self.deployment_log.end = datetime.utcnow()
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
            operation=operation, start=start, end=end, state=state.value, logs=logs
        )

    def _run_operations(self, operation_names, restart=False):
        for operation_name in operation_names:
            operation = self.dag.collections.operations[operation_name]
            if not operation.noop:
                if restart:
                    operation_name = operation_name.replace("_start", "_restart")
                    operation = self.dag.collections.operations[operation_name]
                operation_file = self.dag.collections[
                    operation.collection_name
                ].operations[operation_name]
                operation_log = self.run(operation_name, operation_file)
                if operation_log.state == StateEnum.FAILURE.value:
                    logger.error(f"Operation {operation_name} failed !")
                    yield operation_log
                    return
                logger.info(f"Operation {operation_name} success")
                yield operation_log

    def run_nodes(self, sources=None, targets=None, node_filter=None, restart=False):
        operation_names = self.dag.get_operations(sources=sources, targets=targets)

        if hasattr(node_filter, "search"):
            operation_names = self.dag.filter_operations_regex(
                operation_names, node_filter
            )
        elif node_filter:
            operation_names = self.dag.filter_operations_glob(
                operation_names, node_filter
            )

        start = datetime.utcnow()
        operation_logs_generator = self._run_operations(
            operation_names, restart=restart
        )

        deployment_log = DeploymentLog(
            sources=sources,
            targets=targets,
            filter=str(node_filter) if node_filter else None,
            start=start,
            state=StateEnum.PENDING.value,
        )

        return OperationIterator(
            self.dag, self._service_managers, deployment_log, operation_logs_generator
        )
