# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.core.collection import CollectionReader
from tdp.core.collections import Collections
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

    collection1 = CollectionReader.from_path(collection_path_1)
    collection2 = CollectionReader.from_path(collection_path_2)
    collections = Collections([collection1, collection2])

    assert collections.dag_operations is not None
    assert "service1_install" in collections.dag_operations
    assert "service1_config" in collections.dag_operations
    assert "service2_install" in collections.dag_operations
    assert "service2_config" in collections.dag_operations

    assert [] == collections.dag_operations["service1_install"].depends_on
    assert sorted(["service1_install", "service2_install"]) == sorted(
        collections.dag_operations["service1_config"].depends_on
    )
    assert [] == collections.dag_operations["service2_install"].depends_on
    assert ["service2_install"] == collections.dag_operations[
        "service2_config"
    ].depends_on
