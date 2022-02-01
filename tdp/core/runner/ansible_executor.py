import io
import logging
import os
import subprocess

from .executor import Executor, StateEnum

logger = logging.getLogger("tdp").getChild("ansible_executor")


class AnsibleExecutor(Executor):
    def __init__(self, playbooks_directory, run_directory=None):
        # TODO configurable via config file
        self._playdir = playbooks_directory
        self._rundir = run_directory

    def execute(self, action):
        playbook_action = os.path.join(self._playdir, action + ".yml")
        command = ["ansible-playbook", playbook_action]
        res = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self._rundir,
            universal_newlines=True,
        )
        state = StateEnum.SUCCESS if res.poll() == 0 else StateEnum.FAILURE
        with io.BytesIO() as byte_stream:
            for stdout_line in iter(res.stdout.readline, ""):
                logger.debug(stdout_line)
                byte_stream.write(bytes(stdout_line, "utf-8"))
            logs = byte_stream.getvalue()
        return state, logs
