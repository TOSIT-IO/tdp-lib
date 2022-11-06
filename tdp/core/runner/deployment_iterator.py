# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime
from typing import Iterator

from tdp.core.models import ServiceLog, StateEnum

logger = logging.getLogger("tdp").getChild("deployment_iterator")


class DeploymentIterator(Iterator):
    def __init__(self, log, operations, run_method, cluster_variables):
        self.log = log
        self._operations = operations
        self._run_operation = run_method
        self._cluster_variables = cluster_variables
        self._iter = iter(self._operations)

    def __next__(self):
        self.log.start_time = datetime.utcnow()
        # TODO: handle better when solving #203
        service_logs = {}
        try:
            while True:
                operation = next(self._iter)
                if not operation.service in service_logs:
                    service_log = ServiceLog(
                        service=operation.service,
                        version=self._cluster_variables[operation.service].version,
                    )
                    service_logs[operation.service] = service_log

                if operation.noop == False:
                    operation_log = self._run_operation(operation)
                    operation_log.deployment = self.log
                    return operation_log
        except StopIteration as e:
            self._fill_deployment_log_at_end(self.log, service_logs)
            raise e
        except Exception as e:
            self._fill_deployment_log_at_end(self.log, service_logs, StateEnum.FAILURE)
            raise e

    def _fill_deployment_log_at_end(self, deployment_log, service_logs, state=None):
        deployment_log.end_time = datetime.utcnow()
        if state is not None:
            deployment_log.state = state
        elif len(deployment_log.operations) > 0:
            deployment_log.state = deployment_log.operations[-1].state
        else:
            # case deployment is finised with only noop performed
            deployment_log.state = StateEnum.SUCCESS
        # TODO: when fixing #203, we should complete the list during the deployment
        deployment_log.services.extend(service_logs.values())
