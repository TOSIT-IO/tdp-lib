from datetime import datetime
import re
import logging

import networkx as nx

from tdp.core.models.action_log import ActionLog
from tdp.core.models.deployment_log import DeploymentLog
from tdp.core.runner.executor import StateEnum

logger = logging.getLogger("tdp").getChild("action_runner")


class ActionRunner:
    """Run actions"""

    def __init__(self, dag, executor):
        self.dag = dag
        self._executor = executor
        self._failed_nodes = []
        self._success_nodes = []
        self._skipped_nodes = []

    def run(self, action):
        logger.debug(f"Running action {action}")
        start = datetime.now()
        state, logs = self._executor.execute(action)
        end = datetime.now()
        if not StateEnum.has_value(state):
            logger.error(
                f"Invalid state ({state}) returned by {self._executor.__class__.__name__}.run('{action}'))"
            )
            state = StateEnum.FAILURE
        return ActionLog(action=action, start=start, end=end, state=state, logs=logs)

    def run_to_node(self, node, filter=None):
        actions = self.dag.get_actions_to_node(node)
        if isinstance(filter, re.Pattern):
            actions = self.dag.filter_actions_regex(actions, filter)
        elif filter:
            actions = self.dag.filter_actions_glob(actions, filter)
        start = datetime.now()
        action_logs = []
        state = StateEnum.SUCCESS
        for action in actions:
            if (
                not self.dag.components[action].noop
                and action not in self._failed_nodes + self._skipped_nodes
            ):
                action_log = self.run(action)
                action_logs.append(action_log)
                if action_log.state == StateEnum.FAILURE:
                    state = StateEnum.FAILURE
                    logger.error(f"Action {action} failed !")
                    self._failed_nodes.append(action)
                    for desc in nx.descendants(self.dag.graph, action):
                        logger.warning(f"Action {desc} will be skipped")
                        self._skipped_nodes.append(desc)
                else:
                    logger.info(f"Action {action} success")
                    self._success_nodes.append(action)
        end = datetime.now()
        return DeploymentLog(
            target=node,
            filter=str(filter),
            start=start,
            end=end,
            state=state,
            actions=action_logs,
        )
