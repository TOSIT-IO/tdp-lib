# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from abc import ABC
from collections.abc import Generator, Iterable, Iterator, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional, TypeVar, Union, overload

from tdp.core.constants import (
    ACTION_NAME_MAX_LENGTH,
    HOST_NAME_MAX_LENGTH,
    OPERATION_NAME_MAX_LENGTH,
)
from tdp.core.entities.entity_name import (
    ServiceComponentName,
    ServiceName,
    parse_entity_name,
)

if TYPE_CHECKING:
    from tdp.core.collections.collection_reader import TDPLibDagNodeModel


@dataclass(frozen=True)
class OperationName:
    entity: Union[ServiceName, ServiceComponentName]
    action: str

    def __post_init__(self):
        if len(self.action) > ACTION_NAME_MAX_LENGTH:
            raise ValueError(
                f"Action '{self.action}' must be less than {ACTION_NAME_MAX_LENGTH} "
                "characters."
            )
        if not self.action:
            raise ValueError("Action name cannot be empty.")
        if len(self.name) > OPERATION_NAME_MAX_LENGTH:
            raise ValueError(
                f"Operation '{self.name}' must be less than {OPERATION_NAME_MAX_LENGTH}"
                " characters."
            )

    @property
    def service(self) -> str:
        return self.entity.service

    @property
    def component(self) -> Optional[str]:
        return getattr(self.entity, "component", None)

    @property
    def name(self) -> str:
        return f"{self.entity.name}_{self.action}"

    @classmethod
    def from_str(cls, name: str) -> OperationName:
        entity_name, action_name = name.rsplit("_", 1)
        entity = parse_entity_name(entity_name)
        return cls(entity, action_name)

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


@dataclass(frozen=True)
class Playbook:
    path: Path
    collection_name: str
    hosts: frozenset[str]

    def __post_init__(self):
        for host_name in self.hosts:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise ValueError(
                    f"Host '{host_name}' must be less than {HOST_NAME_MAX_LENGTH} "
                    "characters."
                )

    @property
    def name(self) -> str:
        return self.path.stem


@dataclass(frozen=True)
class Operation(ABC):
    """An operation.

    Not meant to be instantiated directly.

    Args:
        name: Name of the operation.
    """

    name: OperationName

    def __post_init__(self):
        if type(self) is Operation:
            raise TypeError("Operation class cannot be instantiated directly.")


@dataclass(frozen=True)
class PlaybookOperation(Operation):
    """An operation that is linked to a playbook.

    Not meant to be instantiated directly.

    Args:
        name: Name of the operation.
        playbook: The playbook that defines the operation.
    """

    playbook: Playbook

    def __post_init__(self):
        if type(self) is PlaybookOperation:
            raise TypeError("PlaybookOperation class cannot be instantiated directly.")


@dataclass(frozen=True)
class OperationNoop(Operation, ABC):
    """An operation that does nothing.

    Args:
        name: Name of the operation.
    """

    def __post_init__(self):
        if type(self) is OperationNoop:
            raise TypeError("OperationNoop class cannot be instantiated directly.")

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if issubclass(cls, PlaybookOperation):
            raise TypeError(
                f"{cls.__name__} cannot inherit both OperationNoop and PlaybookOperation."
            )


@dataclass(frozen=True)
class DagOperation(Operation, ABC):
    """An operation that is part of the DAG.

    Not meant to be instantiated directly.

    Args:
        name: Name of the operation.
        depends_on: List of operations that must be executed before this one.
    """

    depends_on: frozenset[OperationName]
    collection_names: tuple[str, ...]

    def __post_init__(self):
        if type(self) is DagOperation:
            raise TypeError("DagOperation class cannot be instantiated directly.")


@dataclass(frozen=True)
class DagOperationNoop(DagOperation, OperationNoop):
    """An operation that is part of the DAG and does nothing.

    Args:
        name: Name of the operation.
        depends_on: List of operations that must be executed before this one.
    """

    pass


@dataclass(frozen=True)
class DagOperationWithPlaybook(DagOperation, PlaybookOperation):
    """An operation that is part of the DAG associated with a playbook.

    Args:
        name: Name of the operation.
        depends_on: List of operations that must be executed before this one.
        playbook: The playbook that defines the operation.
    """

    pass


@dataclass
class DagOperationBuilder:
    """A builder for a DAG Operation.

    Meant to be short-lived. Allows to aggregate multiple ReadDagNode.

    Args:
        name: Name of the operation.
        depends_on: List of operations that must be executed before this one.
        playbook: The playbook that defines the operation.
    """

    name: str
    depends_on: set[OperationName]
    collection_names: list[str]
    playbook: Optional[Playbook] = None

    @classmethod
    def from_read_dag_node(
        cls,
        *,
        collection_name: str,
        dag_node: TDPLibDagNodeModel,
        playbook: Optional[Playbook] = None,
    ) -> DagOperationBuilder:
        return cls(
            name=dag_node.name,
            depends_on=set(
                OperationName.from_str(dependency) for dependency in dag_node.depends_on
            ),
            playbook=playbook,
            collection_names=[collection_name],
        )

    def extends(self, dag_node: TDPLibDagNodeModel, collection_name: str) -> None:
        self.depends_on.update(
            OperationName.from_str(dependency) for dependency in dag_node.depends_on
        )
        self.collection_names.append(collection_name)

    def build(self) -> Union[DagOperationNoop, DagOperationWithPlaybook]:
        if self.playbook:
            return DagOperationWithPlaybook(
                name=OperationName.from_str(self.name),
                depends_on=frozenset(self.depends_on),
                playbook=self.playbook,
                collection_names=tuple(self.collection_names),
            )
        return DagOperationNoop(
            name=OperationName.from_str(self.name),
            depends_on=frozenset(self.depends_on),
            collection_names=tuple(self.collection_names),
        )


@dataclass(frozen=True)
class ForgedDagOperation(DagOperation, ABC):
    """An operation that is part of the DAG.

    Not meant to be instantiated directly.

    Args:
        name: Name of the operation.
        depends_on: List of operations that must be executed before this one.
        forged_from: The operation that was forged.
    """

    forged_from: DagOperation

    def __post_init__(self):
        if type(self) is ForgedDagOperation:
            raise TypeError("ForgedDagOperation class cannot be instantiated directly.")

    @staticmethod
    def create(
        operation_name: OperationName,
        source_operation: DagOperation,
        playbook: Optional[Playbook] = None,
    ) -> Union[ForgedDagOperationNoop, ForgedDagOperationWithPlaybook]:
        """Forge a DAG operation."""
        if not isinstance(source_operation, DagOperation):
            raise ValueError(f"Operation {source_operation} cannot be forged.")
        if playbook:
            return ForgedDagOperationWithPlaybook(
                name=operation_name,
                playbook=playbook,
                depends_on=source_operation.depends_on,
                forged_from=source_operation,
                collection_names=source_operation.collection_names,
            )
        return ForgedDagOperationNoop(
            name=operation_name,
            depends_on=source_operation.depends_on,
            forged_from=source_operation,
            collection_names=source_operation.collection_names,
        )


@dataclass(frozen=True)
class ForgedDagOperationNoop(DagOperationNoop, ForgedDagOperation):
    """An operation that is part of the DAG and does nothing.

    Created from a start operation.

    Args:
        name: Name of the operation.
        depends_on: List of operations that must be executed before this one.
        forged_from: The operation that was forged.
    """

    pass


@dataclass(frozen=True)
class ForgedDagOperationWithPlaybook(DagOperationWithPlaybook, ForgedDagOperation):
    """An operation that is part of the DAG associated with a playbook.

    Created from a start operation.

    Args:
        name: Name of the operation.
        playbook: The playbook that defines the operation.
        depends_on: List of operations that must be executed before this one.
        forged_from: The operation that was forged.
    """

    pass


@dataclass(frozen=True)
class OtherPlaybookOperation(PlaybookOperation):
    """An operation that is not part of the DAG.

    Args:
        name: Name of the operation.
        playbook: The playbook that defines the operation.
    """

    pass


T = TypeVar("T", bound=Operation)


class Operations(MutableMapping[Union[OperationName, str], Operation]):
    def __init__(self) -> None:
        self._inner = {}

    def __getitem__(self, key: Union[OperationName, str]):
        if isinstance(key, str):
            key = OperationName.from_str(key)
        try:
            return self._inner[key]
        except KeyError:
            raise KeyError(f"Operation '{key}' not found")

    def __setitem__(self, key: Union[OperationName, str], value: Operation):
        if isinstance(key, str):
            key = OperationName.from_str(key)
        if key != value.name:
            raise ValueError(
                f"Operation name '{value.name}' does not match key '{key}'"
            )
        self._inner[key] = value

    def __delitem__(self, key: Union[OperationName, str]):
        if isinstance(key, str):
            key = OperationName.from_str(key)
        del self._inner[key]

    def __iter__(self) -> Iterator[OperationName]:
        return iter(self._inner)

    def __len__(self) -> int:
        return len(self._inner)

    def add(self, operation: Operation) -> None:
        """Add an operation."""
        self[operation.name] = operation

    @overload
    def get_by_class(
        self, include: Optional[None] = None, exclude: Optional[None] = None
    ) -> Generator[Operation, None, None]: ...

    @overload
    def get_by_class(
        self,
        include: Optional[Union[type[T], Iterable[type[T]]]] = None,
        exclude: Optional[None] = None,
    ) -> Generator[T, None, None]: ...

    @overload
    def get_by_class(
        self,
        include: Optional[None] = None,
        exclude: Optional[Union[type[Operation], Iterable[type[Operation]]]] = None,
    ) -> Generator[Operation, None, None]: ...

    @overload
    def get_by_class(
        self,
        include: Optional[Union[type[T], Iterable[type[T]]]] = None,
        exclude: Optional[Union[type[T], Iterable[type[T]]]] = None,
    ) -> Generator[T, None, None]: ...

    def get_by_class(
        self,
        include: Optional[Union[type[T], Iterable[type[T]]]] = None,
        exclude: Optional[Union[type[Operation], Iterable[type[Operation]]]] = None,
    ) -> Generator[Operation, None, None]:
        # Normalize include and exclude into sets
        include_set = {include} if isinstance(include, type) else set(include or [])
        exclude_set = {exclude} if isinstance(exclude, type) else set(exclude or [])

        for operation in self.values():
            if include_set and not any(
                isinstance(operation, inc) for inc in include_set
            ):
                continue
            if any(isinstance(operation, exc) for exc in exclude_set):
                continue
            yield operation
