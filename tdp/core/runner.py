import os
import subprocess
import logging

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from pathlib import Path
from functools import wraps

logger = logging.getLogger("tdp").getChild("runner")

class Runner:
    """Run actions"""

    def __init__(self, playbooks_directory=None, run_directory=None):
        # TODO configurable via config file
        self._playdir = playbooks_directory
        self._rundir = run_directory


    def run(self, action):
        logger.debug(f"Running action {action}")
        command = []
        command.append('ansible-playbook')
        command.append(os.path.join(self._playdir, action+'.yml'))

        res = subprocess.Popen(
            command, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT,
            cwd=self._rundir,
            universal_newlines=True
        )

        print('STDOUT:')
        for stdout_line in iter(res.stdout.readline, ""):
            print(stdout_line, end='')

        status = {}
        if res.poll() == 0:
            status['is_failed'] = False
        else:
            status['is_failed'] = True

        return status

