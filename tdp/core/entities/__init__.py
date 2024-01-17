# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, ABCMeta, abstractmethod
from dataclasses import is_dataclass


# Custom metaclass to enforce dataclass inheritance
class DataclassEnforcer(ABCMeta):
    def __new__(cls, name, bases, namespace):
        new_class = super().__new__(cls, name, bases, namespace)
        if not is_dataclass(new_class) and any(
            isinstance(b, DataclassEnforcer) for b in bases
        ):
            raise TypeError(f"{name} must be a dataclass")
        return new_class


class NamedEntity(ABC, metaclass=DataclassEnforcer):
    @property
    @abstractmethod
    def name(self) -> str:
        pass
