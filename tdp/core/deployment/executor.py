# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Tuple

from tdp.core.models import OperationStateEnum


class Executor(ABC):
    """An Executor is an object able to run operations."""

    @abstractmethod
    def execute(self, operation: str) -> Tuple[OperationStateEnum, bytes]:
        """Executes an operation.

        Args:
            operation: Operation name.

        Returns:
            Whether an operation is a success as well as its logs in UTF-8 bytes.
        """
        pass
