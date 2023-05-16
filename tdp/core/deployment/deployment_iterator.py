# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime

from tdp.core.models import ServiceComponentLog, StateEnum

logger = logging.getLogger("tdp").getChild("deployment_iterator")


class _Flags:
    def __init__(self, configured=False, started=False):
        self.configured = configured
        self.started = started


class DeploymentIterator(Iterator):
    def __init__(self, log, operations, run_method, cluster_variables):
        self.log = log
        self._operations = operations
        self._run_operation = run_method
        self._cluster_variables = cluster_variables
        self._iter = iter(self._operations)
        self._failed = False
        self._service_component_logs = defaultdict(_Flags)
        self.log.start_time = datetime.utcnow()

    def __next__(self):
        try:
            while True:
                if self._failed == True:
                    raise StopIteration()
                operation = next(self._iter)
                service_component = self._service_component_logs[
                    (operation.service_name, operation.component_name)
                ]

                service_component_log = None
                if operation.action_name == "config":
                    service_component.configured = True

                if (
                    operation.action_name in ("start", "restart")
                    and service_component.configured == True
                    and service_component.started == False
                ):
                    service_component_log = ServiceComponentLog(
                        service=operation.service_name,
                        component=operation.component_name,
                        version=self._cluster_variables[operation.service_name].version,
                    )
                    service_component_log.deployment = self.log

                    service_component.started = True

                operation_log = None
                if operation.noop == False:
                    operation_log = self._run_operation(operation)
                    operation_log.deployment = self.log
                    self._failed = operation_log.state == StateEnum.FAILURE

                return operation_log, service_component_log
        # StopIteration is a "normal" exception raised when the iteration has stopped
        except StopIteration as e:
            self.log.end_time = datetime.utcnow()
            if len(self.log.operations) > 0:
                self.log.state = self.log.operations[-1].state
            else:
                # case deployment is finised with only noop performed
                self.log.state = StateEnum.SUCCESS
            raise e
        # An unforeseen error has occured, stop the deployment and set as failure
        except Exception as e:
            self.log.end_time = datetime.utcnow()
            self.log.state = StateEnum.FAILURE
            raise e
