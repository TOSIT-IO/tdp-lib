from abc import ABC, abstractmethod, abstractstaticmethod
import re

REGEX = re.compile(r"(?<!^)(?=[A-Z])")


class Command(ABC):
    def __init__(self, adapter, args, dag):
        self.adapter = adapter
        self.args = args
        self.dag = dag

    @classmethod
    def command(cls) -> str:
        name = cls.__name__[: -len("Command")]
        name = REGEX.sub("_", name)
        return name.lower()

    @abstractmethod
    def run(self):
        pass

    @abstractstaticmethod
    def fill_argument_definition(parser, dag):
        pass

    def __str__(self) -> str:
        return f"{self.__class__.__name__}: { ', '.join(f'{arg}={value}' for arg, value in vars(self.args).items())}"

    def __repr__(self) -> str:
        return f"<{self.__str__()}>"
