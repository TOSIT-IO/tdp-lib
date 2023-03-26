# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest
import yaml

from tdp.conftest import generate_collection
from tdp.core.collection import Collection
from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.variables import ClusterVariables


@pytest.fixture(scope="session")
def minimal_collections(tmp_path_factory):
    collection_path = tmp_path_factory.mktemp("minimal_collection")
    dag_service_operations = {
        "mock": [
            {"name": "mock_node_install", "depends_on": []},
            {"name": "mock_node_config", "depends_on": ["mock_node_install"]},
            {"name": "mock_node_start", "depends_on": ["mock_node_config"]},
            {"name": "mock_node_init", "depends_on": ["mock_node_start"]},
            {"name": "mock_install", "noop": True, "depends_on": ["mock_node_install"]},
            {
                "name": "mock_config",
                "noop": True,
                "depends_on": ["mock_install", "mock_node_config"],
            },
            {
                "name": "mock_start",
                "noop": True,
                "depends_on": ["mock_config", "mock_node_start"],
            },
            {
                "name": "mock_init",
                "noop": True,
                "depends_on": ["mock_start", "mock_node_init"],
            },
        ],
    }
    service_vars = {
        "mock": {
            "mock": {
                "key": "value",
                "another_key": "another_value",
            },
        },
    }
    generate_collection(collection_path, dag_service_operations, service_vars)
    return Collections.from_collection_list([Collection.from_path(collection_path)])


@pytest.fixture(scope="module")
def cluster_variables(tmp_path_factory: pytest.TempPathFactory, minimal_collections):
    tdp_vars = tmp_path_factory.mktemp("tdp_vars")
    cluster_variables = ClusterVariables.initialize_cluster_variables(
        minimal_collections, tdp_vars
    )

    return cluster_variables


@pytest.fixture(scope="function")
def reconfigurable_cluster_variables(
    tmp_path_factory: pytest.TempPathFactory, minimal_collections
):
    tdp_vars = tmp_path_factory.mktemp("tdp_vars")
    cluster_variables = ClusterVariables.initialize_cluster_variables(
        minimal_collections, tdp_vars
    )
    service_component_deployed_version = [
        ("mock", None, cluster_variables["mock"].version),
        ("mock", "node", cluster_variables["mock"].version),
    ]

    with cluster_variables["mock"].open_var_files(
        "update service configuration", ["mock.yml"]
    ) as configuration:
        configuration["mock.yml"].merge({"test": 1})
    return (cluster_variables, service_component_deployed_version)


@pytest.fixture(scope="session")
def dag(minimal_collections):
    return Dag(minimal_collections)
