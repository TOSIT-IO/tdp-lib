# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import io
import logging
import subprocess
from typing import Tuple

from tdp.core.models import OperationStateEnum

from .executor import Executor

logger = logging.getLogger("tdp").getChild("ansible_executor")


class AnsibleExecutor(Executor):
    """Executor that runs ansible commands."""

    def __init__(self, run_directory=None, dry: bool = False):
        """Initialize the executor.

        Args:
            run_directory: Directory where to run the ansible command.
            dry: Whether or not to run the command in dry mode.
        """
        # TODO configurable via config file
        self._rundir = run_directory
        self._dry = dry

    def _execute_ansible_command(self, command: str) -> Tuple[OperationStateEnum, str]:
        """Execute an ansible command.

        Args:
            command: Command to execute.

        Returns:
            A tuple with the state of the command and the output of the command.
        """
        with io.BytesIO() as byte_stream:
            try:
                res = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=self._rundir,
                    universal_newlines=True,
                )
                if res.stdout is None:
                    raise Exception("Process has not stdout")
                for stdout_line in iter(res.stdout.readline, ""):
                    print(stdout_line, end="")
                    byte_stream.write(bytes(stdout_line, "utf-8"))
                state = (
                    OperationStateEnum.SUCCESS
                    if res.wait() == 0
                    else OperationStateEnum.FAILURE
                )
            except KeyboardInterrupt:
                logger.debug("KeyboardInterrupt caught")
                byte_stream.write(b"\nKeyboardInterrupt")
                return OperationStateEnum.FAILURE, byte_stream.getvalue()
            return state, byte_stream.getvalue()

    def execute(self, operation: str) -> Tuple[OperationStateEnum, str]:
        command = ["ansible-playbook", str(operation)]
        if self._dry:
            logger.info("[DRY MODE] Ansible command: " + " ".join(command))
            return OperationStateEnum.SUCCESS, b""
        return self._execute_ansible_command(command)
