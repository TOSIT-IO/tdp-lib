# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import io
import logging
import subprocess
from typing import Iterable, Optional, Tuple

from tdp.core.models import OperationStateEnum

logger = logging.getLogger("tdp").getChild("ansible_executor")


class Executor:
    """Allow to execute commands using Ansible."""

    def __init__(self, run_directory=None, dry: bool = False):
        """Initialize the executor.

        Args:
            run_directory: Directory where to run the ansible command.
            dry: Whether or not to run the command in dry mode.
        """
        # TODO configurable via config file
        self._rundir = run_directory
        self._dry = dry

    def _execute_ansible_command(
        self, command: list[str]
    ) -> Tuple[OperationStateEnum, bytes]:
        """Execute an ansible command.

        Args:
            command: Command to execute with args.

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

    def execute(
        self,
        playbook: str,
        host: Optional[str] = None,
        extra_vars: Optional[Iterable[str]] = None,
    ) -> Tuple[OperationStateEnum, bytes]:
        """Executes a playbook.

        Args:
            playbook: Name of the playbook to execute.
            host: Host where the playbook must be ran.

        Returns:
            A tuple with the state of the command and the output of the command in UTF-8.
        """
        command = ["ansible-playbook", str(playbook)]
        if host is not None:
            command.extend(["--limit", host])
        if extra_vars is not None:
            for extra_var in extra_vars:
                command.extend(["--extra-vars", extra_var])
        if self._dry:
            logger.info("[DRY MODE] Ansible command: " + " ".join(command))
            return OperationStateEnum.SUCCESS, b""
        return self._execute_ansible_command(command)
