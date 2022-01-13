import os
import re
import subprocess
import logging

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pathlib import Path
from functools import wraps

import networkx as nx

logger = logging.getLogger("tdp").getChild("runner")


class Runner:
    """Run actions"""

    def __init__(self, dag, playbooks_directory=None, run_directory=None):
        self.dag = dag
        # TODO configurable via config file
        self._playdir = playbooks_directory
        self._rundir = run_directory

        self._failed_nodes = []
        self._success_nodes = []
        self._skipped_nodes = []

    def run(self, action):
        logger.debug(f"Running action {action}")
        command = []
        command.append("ansible-playbook")
        command.append(os.path.join(self._playdir, action + ".yml"))

        res = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self._rundir,
            universal_newlines=True,
        )

        print("STDOUT:")
        for stdout_line in iter(res.stdout.readline, ""):
            print(stdout_line, end="")

        status = {}
        if res.poll() == 0:
            status["is_failed"] = False
        else:
            status["is_failed"] = True

        return status

    def run_to_node(self, node, filter=None):
        actions = self.dag.get_actions_to_node(node)
        if isinstance(filter, re.Pattern):
            actions = self.dag.filter_actions_regex(actions, filter)
        elif filter:
            actions = self.dag.filter_actions_glob(actions, filter)

        for action in actions:
            if (
                not self.dag.components[action].noop
                and action not in self._failed_nodes + self._skipped_nodes
            ):
                res = self.run(action)
                if res["is_failed"]:
                    logger.error(f"Action {action} failed !")
                    self._failed_nodes.append(action)
                    for desc in nx.descendants(self.dag.graph, action):
                        logger.warning(f"Action {desc} will be skipped")
                        self._skipped_nodes.append(desc)

                else:
                    logger.info(f"Action {action} success")
                    self._success_nodes.append(action)
