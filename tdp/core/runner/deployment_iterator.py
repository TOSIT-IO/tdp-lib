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
        self._failed = False
        self._service_logs = {}
        self.log.start_time = datetime.utcnow()

    def __next__(self):
        # TODO: handle better when solving #203
        try:
            while True:
                if self._failed == True:
                    raise StopIteration()
                operation = next(self._iter)
                if not operation.service in self._service_logs:
                    service_log = ServiceLog(
                        service=operation.service,
                        version=self._cluster_variables[operation.service].version,
                    )
                    self._service_logs[operation.service] = service_log

                if operation.noop == False:
                    operation_log = self._run_operation(operation)
                    operation_log.deployment = self.log
                    self._failed = operation_log.state == StateEnum.FAILURE
                    return operation_log
        except StopIteration as e:
            self._fill_deployment_log_at_end(self.log)
            raise e
        except Exception as e:
            self._fill_deployment_log_at_end(self.log, StateEnum.FAILURE)
            raise e

    def _fill_deployment_log_at_end(self, deployment_log, state=None):
        deployment_log.end_time = datetime.utcnow()
        if state is not None:
            deployment_log.state = state
        elif len(deployment_log.operations) > 0:
            deployment_log.state = deployment_log.operations[-1].state
        else:
            # case deployment is finised with only noop performed
            deployment_log.state = StateEnum.SUCCESS
        # TODO: when fixing #203, we should complete the list during the deployment
        deployment_log.services.extend(self._service_logs.values())
