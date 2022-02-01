from datetime import datetime
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
        elif not isinstance(state, StateEnum):
            state = StateEnum(state)

        return ActionLog(action=action, start=start, end=end, state=state, logs=logs)

    def _run_actions(self, actions):
        for action in actions:
            if (
                not self.dag.components[action].noop
                and action not in self._failed_nodes + self._skipped_nodes
            ):
                action_log = self.run(action)
                if action_log.state == StateEnum.FAILURE:
                    logger.error(f"Action {action} failed !")
                    self._failed_nodes.append(action)
                    descendants = nx.descendants(self.dag.graph, action).intersection(
                        actions
                    )
                    self._skipped_nodes.extend(descendants)
                    for desc in descendants:
                        logger.warning(f"Action {desc} will be skipped")
                else:
                    logger.info(f"Action {action} success")
                    self._success_nodes.append(action)
                yield action_log

    def run_to_node(self, node, node_filter=None):
        actions = self.dag.get_actions_to_node(node)

        if hasattr(node_filter, "search"):
            actions = self.dag.filter_actions_regex(actions, node_filter)
        elif node_filter:
            actions = self.dag.filter_actions_glob(actions, node_filter)

        start = datetime.now()
        action_logs = list(self._run_actions(actions))
        end = datetime.now()

        filtered_failures = filter(
            lambda action_log: action_log.state == StateEnum.FAILURE, action_logs
        )

        state = StateEnum.FAILURE if any(filtered_failures) else StateEnum.SUCCESS

        return DeploymentLog(
            target=node,
            filter=str(node_filter),
            start=start,
            end=end,
            state=state,
            actions=action_logs,
        )
