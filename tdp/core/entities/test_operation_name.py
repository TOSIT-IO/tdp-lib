from tdp.core.entities.operation_name import (
    ActionName,
    ServiceComponentOperationName,
    ServiceOperationName,
    parse_operation_name,
)


def test_operation_name():
    action_name = ActionName("a")

    assert isinstance(action_name, ActionName)
    assert str(action_name) == "a"


def test_create_operation_name_from_service_operation_name():
    operation_name = parse_operation_name("s_a")
    assert isinstance(operation_name, ServiceOperationName)
    assert str(operation_name) == "s_a"
    assert str(operation_name._action) == "a"
    assert str(operation_name.service) == "s"


def test_create_operation_name_from_service_component_operation_name():
    operation_name = parse_operation_name("s_c_a")
    assert isinstance(operation_name, ServiceComponentOperationName)
    assert str(operation_name) == "s_c_a"
    assert str(operation_name.action) == "a"
    assert str(operation_name.service) == "s"
    assert str(operation_name.service_component) == "s_c"
