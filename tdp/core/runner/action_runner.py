# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime

from tdp.core.models.action_log import ActionLog
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.models.service_log import ServiceLog
from tdp.core.runner.executor import StateEnum

logger = logging.getLogger("tdp").getChild("action_runner")


class ActionRunner:
    """Run actions"""

    def __init__(self, dag, executor, service_managers):
        self.dag = dag
        self._executor = executor
        self._service_managers = service_managers

    def run(self, action, action_file):
        logger.debug(f"Running action {action}")

        start = datetime.utcnow()
        state, logs = self._executor.execute(action_file)
        end = datetime.utcnow()

        if not StateEnum.has_value(state):
            logger.error(
                f"Invalid state ({state}) returned by {self._executor.__class__.__name__}.run('{action_file}'))"
            )
            state = StateEnum.FAILURE
        elif not isinstance(state, StateEnum):
            state = StateEnum(state)

        return ActionLog(
            action=action, start=start, end=end, state=state.value, logs=logs
        )

    def _run_actions(self, actions):
        for action in actions:
            component = self.dag.components[action]
            if not component.noop:
                action_file = self.dag.collections[component.collection_name].actions[
                    action
                ]
                action_log = self.run(action, action_file)
                if action_log.state == StateEnum.FAILURE.value:
                    logger.error(f"Action {action} failed !")
                    yield action_log
                    return
                logger.info(f"Action {action} success")
                yield action_log

    def _services_from_actions(self, actions):
        """Returns a set of services from an action list"""
        return {
            self.dag.components[action].service
            for action in actions
            if not self.dag.components[action].noop
        }

    def _build_service_logs(self, action_logs):
        services = self._services_from_actions(
            action_log.action for action_log in action_logs
        )
        return [
            ServiceLog(
                service=self._service_managers[service_name].name,
                version=self._service_managers[service_name].version,
            )
            for service_name in services
        ]

    def run_nodes(self, sources=None, targets=None, node_filter=None):
        actions = self.dag.get_actions(sources=sources, targets=targets)

        if hasattr(node_filter, "search"):
            actions = self.dag.filter_actions_regex(actions, node_filter)
        elif node_filter:
            actions = self.dag.filter_actions_glob(actions, node_filter)

        start = datetime.utcnow()
        action_logs = list(self._run_actions(actions))
        end = datetime.utcnow()

        filtered_failures = filter(
            lambda action_log: action_log.state == StateEnum.FAILURE.value, action_logs
        )

        state = StateEnum.FAILURE if any(filtered_failures) else StateEnum.SUCCESS

        service_logs = self._build_service_logs(action_logs)
        return DeploymentLog(
            sources=sources,
            targets=targets,
            filter=str(node_filter),
            start=start,
            end=end,
            state=state.value,
            actions=action_logs,
            services=service_logs,
        )
