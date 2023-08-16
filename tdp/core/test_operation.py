# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.core.operation import (
    ACTION_NAME_MAX_LENGTH,
    COMPONENT_NAME_MAX_LENGTH,
    HOST_NAME_MAX_LENGTH,
    OPERATION_NAME_MAX_LENGTH,
    SERVICE_NAME_MAX_LENGTH,
    InvalidActionNameError,
    InvalidComponentNameError,
    InvalidHostNameError,
    InvalidOperationNameError,
    InvalidServiceNameError,
    Operation,
    OperationName,
    ServiceComponentName,
)


def too_long_name(length: int) -> str:
    return "a" * (length + 1)


class TestServiceComponentName:
    def test_init(self):
        service_component_name = ServiceComponentName("service", "component")
        assert service_component_name.service_name == "service"
        assert service_component_name.component_name == "component"
        assert service_component_name.full_name == "service_component"
        assert not service_component_name.is_service()

    def test_init_without_component(self):
        service_component_name = ServiceComponentName("service")
        assert service_component_name.service_name == "service"
        assert service_component_name.component_name == ""
        assert service_component_name.full_name == "service"
        assert service_component_name.is_service()

    def test_from_full_name(self):
        service_component_name = ServiceComponentName.from_full_name(
            "service_component"
        )
        assert service_component_name.service_name == "service"
        assert service_component_name.component_name == "component"
        assert service_component_name.full_name == "service_component"
        assert not service_component_name.is_service()

    def test_from_full_name_without_component(self):
        service_component_name = ServiceComponentName.from_full_name("service")
        assert service_component_name.service_name == "service"
        assert service_component_name.component_name == ""
        assert service_component_name.full_name == "service"
        assert service_component_name.is_service()

    def test_from_full_name_with_long_component(self):
        service_component_name = ServiceComponentName.from_full_name(
            "service_long_component_name"
        )
        assert service_component_name.service_name == "service"
        assert service_component_name.component_name == "long_component_name"
        assert service_component_name.full_name == "service_long_component_name"
        assert not service_component_name.is_service()

    def test_from_empty_full(self):
        with pytest.raises(InvalidServiceNameError):
            ServiceComponentName.from_full_name("")

    def test_with_missing_service(self):
        with pytest.raises(InvalidServiceNameError):
            ServiceComponentName.from_full_name("_component")

    def test_with_missing_component(self):
        with pytest.raises(InvalidComponentNameError):
            ServiceComponentName.from_full_name("service_")

    def test_with_missing_service_and_component(self):
        with pytest.raises(InvalidServiceNameError):
            ServiceComponentName.from_full_name("_")

    def test_with_too_long_service(self):
        with pytest.raises(InvalidServiceNameError):
            ServiceComponentName.from_full_name(too_long_name(SERVICE_NAME_MAX_LENGTH))

    def test_with_too_long_component(self):
        with pytest.raises(InvalidComponentNameError):
            ServiceComponentName.from_full_name(
                f"service_{too_long_name(COMPONENT_NAME_MAX_LENGTH)}"
            )

    def test__eq__(self):
        service_component_name = ServiceComponentName("service", "component")
        assert service_component_name == ServiceComponentName("service", "component")
        assert service_component_name != ServiceComponentName("service", "other")
        assert service_component_name != ServiceComponentName("other", "component")
        assert service_component_name != ServiceComponentName("other", "other")


class TestOperationName:
    def test_init(self):
        operation_name = OperationName("service", "component", "action")
        assert operation_name.full_name == "service_component_action"
        assert operation_name.service_name == "service"
        assert operation_name.component_name == "component"
        assert operation_name.action_name == "action"
        assert operation_name.is_service() is False

    def test_init_without_component(self):
        operation_name = OperationName("service", "", "action")
        assert operation_name.full_name == "service_action"
        assert operation_name.service_name == "service"
        assert operation_name.component_name == ""
        assert operation_name.action_name == "action"
        assert operation_name.is_service() is True

    def test_from_full_name(self):
        operation_name = OperationName.from_full_name("service_component_action")
        assert operation_name.full_name == "service_component_action"
        assert operation_name.service_name == "service"
        assert operation_name.component_name == "component"
        assert operation_name.action_name == "action"
        assert operation_name.is_service() is False

    def test_from_full_name_without_component(self):
        operation_name = OperationName.from_full_name("service_action")
        assert operation_name.full_name == "service_action"
        assert operation_name.service_name == "service"
        assert operation_name.component_name == ""
        assert operation_name.action_name == "action"
        assert operation_name.is_service() is True

    def test_from_full_name_with_long_component(self):
        operation_name = OperationName.from_full_name(
            "service_long_component_name_action"
        )
        assert operation_name.full_name == "service_long_component_name_action"
        assert operation_name.service_name == "service"
        assert operation_name.component_name == "long_component_name"
        assert operation_name.action_name == "action"
        assert operation_name.is_service() is False

    def test_from_empty_full_name(self):
        with pytest.raises(InvalidOperationNameError):
            OperationName.from_full_name("")

    def test_from_full_name_with_service_only(self):
        with pytest.raises(InvalidOperationNameError):
            OperationName.from_full_name("service")

    def from_full_name_without_service(self):
        with pytest.raises(InvalidServiceNameError):
            OperationName.from_full_name("_component_action")

    def from_full_name_without_component(self):
        with pytest.raises(InvalidComponentNameError):
            OperationName.from_full_name("service__action")

    def from_full_name_without_action(self):
        with pytest.raises(InvalidActionNameError):
            OperationName.from_full_name("service_component_")

    def from_full_name_without_service_and_component(self):
        with pytest.raises(InvalidServiceNameError):
            OperationName.from_full_name("__action")

    def from_full_name_without_service_and_action(self):
        with pytest.raises(InvalidServiceNameError):
            OperationName.from_full_name("_component_")

    def from_full_name_without_component_and_action(self):
        with pytest.raises(InvalidComponentNameError):
            OperationName.from_full_name("service__")

    def from_full_name_without_service_and_component_and_action(self):
        with pytest.raises(InvalidServiceNameError):
            OperationName.from_full_name("___")

    def test_with_too_long_service(self):
        with pytest.raises(InvalidServiceNameError):
            OperationName.from_full_name(
                too_long_name(SERVICE_NAME_MAX_LENGTH) + "_component_action"
            )

    def test_with_too_long_component(self):
        with pytest.raises(InvalidComponentNameError):
            OperationName.from_full_name(
                "service_" + too_long_name(COMPONENT_NAME_MAX_LENGTH) + "_action"
            )

    def test_with_too_long_action(self):
        with pytest.raises(InvalidActionNameError):
            OperationName.from_full_name(
                "service_component_" + too_long_name(ACTION_NAME_MAX_LENGTH)
            )

    def test_with_too_long_operation_name(self):
        with pytest.raises(InvalidOperationNameError):
            OperationName.from_full_name(too_long_name(OPERATION_NAME_MAX_LENGTH))

    def test__eq__(self):
        operation_name = OperationName("service", "component", "action")
        assert operation_name == OperationName("service", "component", "action")
        assert operation_name != OperationName("service", "component", "other")
        assert operation_name != OperationName("service", "other", "action")
        assert operation_name != OperationName("service", "other", "other")
        assert operation_name != OperationName("other", "component", "action")
        assert operation_name != OperationName("other", "component", "other")
        assert operation_name != OperationName("other", "other", "action")
        assert operation_name != OperationName("other", "other", "other")


class TestOperation:
    def test_minimal_init(self):
        operation = Operation(
            name="service_component_action", collection_name="collection"
        )
        assert operation.full_name == "service_component_action"
        assert operation.service_name == "service"
        assert operation.component_name == "component"
        assert operation.action_name == "action"
        assert operation.is_service_operation() is False
        assert operation.collection_name == "collection"
        assert operation.depends_on == []
        assert operation.host_names == set()
        assert operation.is_noop() is False

    def test_init(self):
        operation = Operation(
            name="service_component_action",
            collection_name="collection",
            depends_on=["service_component_other"],
            host_names=set(["host1", "host2"]),
            noop=True,
        )
        assert operation.full_name == "service_component_action"
        assert operation.service_name == "service"
        assert operation.component_name == "component"
        assert operation.action_name == "action"
        assert operation.is_service_operation() is False
        assert operation.collection_name == "collection"
        assert operation.depends_on == ["service_component_other"]
        assert operation.host_names == set(["host1", "host2"])
        assert operation.is_noop() is True

    def test_init_with_long_host_name(self):
        with pytest.raises(InvalidHostNameError):
            Operation(
                name="service_component_action",
                collection_name="collection",
                depends_on=["service_component_other"],
                host_names=set([too_long_name(HOST_NAME_MAX_LENGTH)]),
                noop=True,
            )
