# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import Optional

SERVICE_NAME_MAX_LENGTH = 20
COMPONENT_NAME_MAX_LENGTH = 30
ACTION_NAME_MAX_LENGTH = 20

OPERATION_NAME_MAX_LENGTH = (
    SERVICE_NAME_MAX_LENGTH + COMPONENT_NAME_MAX_LENGTH + ACTION_NAME_MAX_LENGTH
)

HOST_NAME_MAX_LENGTH = 255


class InvalidOperationNameError(ValueError):
    pass


class InvalidServiceNameError(ValueError):
    pass


class InvalidComponentNameError(ValueError):
    pass


class InvalidActionNameError(ValueError):
    pass


class InvalidHostNameError(ValueError):
    pass


class ServiceComponentName:
    """Represent a service or a component name.

    This class is primarly made to parse a <service>_<component> string.

    Example:
        >>> ServiceComponentName.from_full_name("service_component")
        ServiceComponentName("service", "component")
        >>> ServiceComponentName.from_full_name("service")
        ServiceComponentName("service", "")

    Attributes:
        service_name: Name of the service.
        component_name: Name of the component.
    """

    def __init__(self, service_name: str, component_name: str = ""):
        if len(service_name) > SERVICE_NAME_MAX_LENGTH:
            raise InvalidServiceNameError(
                f"Service name '{service_name}' cannot be longer than "
                + "{SERVICE_NAME_MAX_LENGTH} characters."
            )
        if (
            not len(component_name) == 0
            and len(component_name) > COMPONENT_NAME_MAX_LENGTH
        ):
            raise InvalidComponentNameError(
                f"Component name '{component_name}' cannot be longer than "
                + "{COMPONENT_NAME_MAX_LENGTH} characters."
            )
        self.service_name = service_name
        self.component_name = component_name

    @property
    def full_name(self) -> str:
        """Full name of the service or component."""
        return (
            self.service_name
            if self.is_service()
            else f"{self.service_name}_{self.component_name}"
        )

    def is_service(self) -> bool:
        """True if the component is a service."""
        return self.component_name == ""

    @staticmethod
    def from_full_name(full_name: str) -> ServiceComponentName:
        """Factory method to build ServiceComponentName from a full name.

        Args:
            full_name: Full name of the service or component (e.g. "service_component").

        Returns:
            ServiceComponentName instance.
        """
        [service_name, *component_name] = full_name.split("_", 1)
        if not service_name:
            # Case "_component" or "_"
            raise InvalidServiceNameError("Service name cannot be empty.")
        if len(component_name) == 0:
            # Case "service"
            component_name = ""
        elif component_name[0] == "":
            # Case "service_"
            raise InvalidComponentNameError("Component name is empty.")
        else:
            # Case "service_component"
            component_name = component_name[0]
        return ServiceComponentName(service_name, component_name)

    def __repr__(self) -> str:
        return f"ServiceComponentName({self.service_name}, {self.component_name})"

    def __str__(self) -> str:
        return self.full_name

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, ServiceComponentName):
            return False
        return (
            self.service_name == __value.service_name
            and self.component_name == __value.component_name
        )


class OperationName(ServiceComponentName):
    """Represent an operation name.

    This class is primarly made to parse a <service>_<component>_<action> string.

    Example:
        >>> OperationName.from_full_name("service_component_action")
        OperationName("service", "component", "action")
        >>> OperationName.from_full_name("service_action")
        OperationName("service", "", "action")

    Attributes:
        service_name: Name of the service.
    """

    def __init__(self, service_name: str, component_name: str, action_name: str):
        super().__init__(service_name, component_name)
        if len(action_name) > ACTION_NAME_MAX_LENGTH:
            raise InvalidActionNameError(
                f"Action name '{action_name}' must be less than "
                + "{ACTION_NAME_MAX_LENGTH} characters."
            )
        self.action_name = action_name
        if len(self.full_name) > OPERATION_NAME_MAX_LENGTH:
            raise InvalidOperationNameError(
                f"Operation name '{self.full_name}' must be less than "
                + "{OPERATION_NAME_MAX_LENGTH} characters."
            )

    @property
    def full_name(self) -> str:
        """Full name of the operation."""
        return f"{super().full_name}_{self.action_name}"

    def is_service_opearion(self) -> bool:
        """True if the operation scope is the whole service."""
        return self.is_service()

    @staticmethod
    def from_full_name(full_name: str) -> OperationName:
        """Factory method to build OperationName from a full name.

        Args:
            full_name: Full name of the operation.

        Returns:
            OperationName instance.
        """
        if "_" not in full_name:
            raise InvalidOperationNameError(f"Invalid operation name '{full_name}'.")
        [service_component_name, action_name] = full_name.rsplit("_", 1)
        service_component_name = ServiceComponentName.from_full_name(
            service_component_name
        )
        return OperationName(
            service_component_name.service_name,
            service_component_name.component_name,
            action_name,
        )

    def __repr__(self) -> str:
        return (
            "OperationName("
            + f"{self.service_name}, "
            + f"{self.component_name}, "
            + f"{self.action_name})"
        )

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, OperationName):
            return False
        return (
            self.service_name == __value.service_name
            and self.component_name == __value.component_name
            and self.action_name == __value.action_name
        )


class Operation:
    """A task that can be executed by Ansible.

    An operation can be defined in an Ansible collection. It can either be part of the
    DAG or not (if it has some depencies defined in the `tdp_lib_dag` directory).

    Operations are instanciated inside the Collections class and are immutable. They
    are meant to be passed to the Ansible executor.
    """

    def __init__(
        self,
        name: str,
        collection_name: str,
        noop: bool = False,
        depends_on: Optional[list[str]] = None,
        host_names: Optional[set[str]] = None,
    ):
        """Create a new Operation.

        Args:
            name: Full name of the operation. It is composed of the service name, the
              component name (optional) and the action name
              (<service>_<component>_<action>).
            collection_name: Name of the collection where the operation is defined.
            depends_on: List of operations that must be executed before this one.
            host_names: Set of host names where the operation can be launched.
            noop: If True, the operation will not be executed.
        """
        self._name = OperationName.from_full_name(name)
        self._collection_name = collection_name
        self._depends_on = depends_on or []
        self._host_names = host_names or set()
        self._noop = noop

        for host_name in self._host_names:
            if len(host_name) > HOST_NAME_MAX_LENGTH:
                raise InvalidHostNameError(
                    f"host {host_name} is longer than {HOST_NAME_MAX_LENGTH}"
                )

    @property
    def full_name(self) -> str:
        """Full name of the operation."""
        return self._name.full_name

    @property
    def service_name(self) -> str:
        """Name of the service."""
        return self._name.service_name

    @property
    def component_name(self) -> str:
        """Name of the component."""
        return self._name.component_name

    @property
    def action_name(self) -> str:
        """Name of the action."""
        return self._name.action_name

    def is_service_operation(self) -> bool:
        """Return True if the operation is about a service, False otherwise."""
        return self._name.is_service()

    @property
    def collection_name(self) -> str:
        """Name of the collection where the operation is defined."""
        return self._collection_name

    @property
    def depends_on(self) -> list[str]:
        """List of operations that must be executed before this one."""
        return self._depends_on

    @property
    def host_names(self) -> set[str]:
        """Set of host names where the operation can be launched."""
        return self._host_names

    def is_noop(self) -> bool:
        """Return True if the operation is a noop, False otherwise."""
        return self._noop

    def __repr__(self):
        return (
            f"Operation(name={self._name}, "
            f"collection_name={self._collection_name}, "
            f"depends_on={self._depends_on}, "
            f"noop={self._noop}, "
            f"host_names={self._host_names})"
        )
