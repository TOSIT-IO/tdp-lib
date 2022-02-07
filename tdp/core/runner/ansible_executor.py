import io
import logging
import os
import subprocess

from .executor import Executor, StateEnum

logger = logging.getLogger("tdp").getChild("ansible_executor")


class AnsibleExecutor(Executor):
    def __init__(self, playbooks_directory, run_directory=None, dry=False):
        # TODO configurable via config file
        self._playdir = playbooks_directory
        self._rundir = run_directory
        self._dry = dry

    def execute(self, action):
        playbook_action = os.path.join(self._playdir, action + ".yml")
        command = ["ansible-playbook", playbook_action]
        if self._dry:
            logger.info("[DRY MODE] Ansible command: " + " ".join(command))
            return StateEnum.SUCCESS, b""

        res = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self._rundir,
            universal_newlines=True,
        )
        with io.BytesIO() as byte_stream:
            for stdout_line in iter(res.stdout.readline, ""):
                print(stdout_line)
                byte_stream.write(bytes(stdout_line, "utf-8"))
            logs = byte_stream.getvalue()
        state = StateEnum.SUCCESS if res.poll() == 0 else StateEnum.FAILURE
        return state, logs
