from tdp.core.entities.operation import PlaybookOperation
from tdp.core.entities.operation_name import parse_operation_name


def test_playbook_operation():
    operation_name = parse_operation_name("s_c_a")
    collection = "c"
    depends_on = set()
    host_names = {"host1", "host2", "host3"}
    operation = PlaybookOperation(operation_name, collection, depends_on, host_names)

    assert isinstance(operation, PlaybookOperation)
    assert operation.host_names == host_names
    assert str(operation.name) == "s_c_a"
    assert str(operation.name.service) == "s"
    assert str(operation.name.action) == "a"
    assert operation.collection_name == collection
    assert operation.depends_on == depends_on
