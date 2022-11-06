# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod


class Executor(ABC):
    """An Executor is an object able to run an operation."""

    @abstractmethod
    def execute(self, operation):
        """Executes an operation

        Args:
            operation (str): Operation name

        Returns:
            Tuple[StateEnum, bytes]: Whether an operation is a success as well as its logs in UTF-8 bytes.
        """
        pass
