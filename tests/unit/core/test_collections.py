# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.core.collections.collections import Collections
from tdp.core.entities.operation import DagOperation, OperationName
from tests.conftest import generate_collection_at_path


def test_collections_from_collection_list(tmp_path_factory: pytest.TempPathFactory):
    collection_path_1 = tmp_path_factory.mktemp("collection1")
    dag_service_operations_1 = {
        "service1": [
            {"name": "service1_install", "depends_on": []},
            {"name": "service1_config", "depends_on": ["service1_install"]},
        ],
    }
    service_vars_1 = {
        "service1": {
            "service1": {},
        },
    }

    collection_path_2 = tmp_path_factory.mktemp("collection2")
    dag_service_operations_2 = {
        "service1": [
            {"name": "service1_config", "depends_on": ["service2_install"]},
        ],
        "service2": [
            {"name": "service2_install", "depends_on": []},
            {"name": "service2_config", "depends_on": ["service2_install"]},
        ],
    }
    service_vars_2 = {
        "service2": {
            "service2": {},
        },
    }

    generate_collection_at_path(
        collection_path_1, dag_service_operations_1, service_vars_1
    )
    generate_collection_at_path(
        collection_path_2, dag_service_operations_2, service_vars_2
    )

    collections = Collections.from_collection_paths(
        [collection_path_1, collection_path_2]
    )

    dag_operations = {
        str(op.name): op for op in collections.operations.get_by_class(DagOperation)
    }

    assert len(dag_operations) != 0
    assert "service1_install" in dag_operations
    assert "service1_config" in dag_operations
    assert "service2_install" in dag_operations
    assert "service2_config" in dag_operations

    assert len(dag_operations["service1_install"].depends_on) == 0
    assert (
        frozenset(
            [
                OperationName.from_str("service1_install"),
                OperationName.from_str("service2_install"),
            ]
        )
        == dag_operations["service1_config"].depends_on
    )
    assert len(dag_operations["service2_install"].depends_on) == 0
    assert (
        frozenset([OperationName.from_str("service2_install")])
        == dag_operations["service2_config"].depends_on
    )
