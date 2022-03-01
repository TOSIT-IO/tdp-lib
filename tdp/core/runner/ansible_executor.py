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

    def _execute_ansible_command(self, command):
        with io.BytesIO() as byte_stream:
            try:
                res = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=self._rundir,
                    universal_newlines=True,
                )
                for stdout_line in iter(res.stdout.readline, ""):
                    print(stdout_line)
                    byte_stream.write(bytes(stdout_line, "utf-8"))
                state = StateEnum.SUCCESS if res.poll() == 0 else StateEnum.FAILURE
            except KeyboardInterrupt:
                logger.debug("KeyboardInterrupt caught")
                byte_stream.write(b"\nKeyboardInterrupt")
                return StateEnum.FAILURE, byte_stream.getvalue()
            return state, byte_stream.getvalue()

    def execute(self, action):
        playbook_action = os.path.join(self._playdir, action + ".yml")
        command = ["ansible-playbook", playbook_action]
        if self._dry:
            logger.info("[DRY MODE] Ansible command: " + " ".join(command))
            return StateEnum.SUCCESS, b""
        return self._execute_ansible_command(command)