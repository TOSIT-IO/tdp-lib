# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import io
import logging
import subprocess
from collections.abc import Iterable
from pathlib import Path
from typing import Optional

from tdp.core.models.enums import OperationStateEnum
from tdp.exception_playbooks import exception_playbooks
from tdp.utils import resolve_executable

logger = logging.getLogger(__name__)


class Executor:
    """Allow to execute commands using Ansible."""

    def __init__(self, run_directory=None, dry: bool = False):
        """Initialize the executor.

        Args:
            run_directory: Directory where to run the ansible command.
            dry: Whether or not to run the command in dry mode.

        Raises:
            ExecutableNotFoundError: If the ansible-playbook command is not found in PATH.
        """
        # TODO configurable via config file
        self._rundir = run_directory
        self._dry = dry

        # Resolve ansible-playbook command
        ansible_playbook_command = "ansible-playbook"
        if self._dry:
            # In dry mode, we don't want to execute the ansible-playbook command
            self.ansible_path = ansible_playbook_command
        else:
            # Check if the ansible-playbook command is available in PATH
            self.ansible_path = resolve_executable(ansible_playbook_command)

    def _execute_ansible_command(
        self, command: list[str]
    ) -> tuple[OperationStateEnum, bytes]:
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
        playbook: Path,
        host: Optional[str] = None,
        extra_vars: Optional[Iterable[str]] = None,
    ) -> tuple[OperationStateEnum, bytes]:
        """Executes a playbook.

        Args:
            playbook: Name of the playbook to execute.
            host: Host where the playbook must be ran.

        Returns:
            A tuple with the state of the command and the output of the command in UTF-8.
        """
        # Build command
        command = [self.ansible_path]
        command += [str(playbook)]
        if host is not None:
            command += [
                "--limit",
                (
                    host
                    if "/".join(str(playbook).rsplit("/", 3)[-3:])
                    not in exception_playbooks
                    else "all"
                ),
            ]
        for extra_var in extra_vars or []:
            command += ["--extra-vars", extra_var]
        # Execute command
        if self._dry:
            # Operation always succeed in dry mode
            logger.debug("[DRY MODE] Ansible command: " + " ".join(command))
            return OperationStateEnum.SUCCESS, b""
        logger.debug("Ansible command: " + " ".join(command))
        return self._execute_ansible_command(command)
