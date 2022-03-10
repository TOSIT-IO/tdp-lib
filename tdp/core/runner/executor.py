from abc import ABC, abstractmethod
from enum import Enum


class StateEnum(Enum):
    SUCCESS = "Success"
    FAILURE = "Failure"

    @classmethod
    def has_value(cls, value):
        return isinstance(value, StateEnum) or value in (
            v.value for v in cls.__members__.values()
        )

    @classmethod
    def max_length(cls):
        return max(len(state.value) for state in list(StateEnum))


class Executor(ABC):
    """An Executor is an object able to run an action."""

    @abstractmethod
    def execute(self, action):
        """Executes an action

        Args:
            action (str): Action name

        Returns:
            Tuple[StateEnum, bytes]: Whether an action is a success as well as its logs in UTF-8 bytes.
        """
        pass
