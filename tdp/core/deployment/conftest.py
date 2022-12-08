# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest
import yaml

from tdp.core.collection import Collection
from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.variables import ClusterVariables

MOCK_SERVICE_DAG = [
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
    {"name": "mock_init", "noop": True, "depends_on": ["mock_start", "mock_node_init"]},
]


@pytest.fixture(scope="session")
def minimal_collections(tmp_path_factory):
    collection = tmp_path_factory.mktemp("minimal_collection")

    tdp_lib_dag = collection / "tdp_lib_dag"
    playbooks = collection / "playbooks"
    tdp_vars_defaults = collection / "tdp_vars_defaults"
    mock_defaults = tdp_vars_defaults / "mock"

    tdp_lib_dag.mkdir()
    playbooks.mkdir()
    tdp_vars_defaults.mkdir()
    mock_defaults.mkdir()

    with (tdp_lib_dag / "mock.yml").open("w") as fd:
        fd.write(yaml.dump(MOCK_SERVICE_DAG))

    # create empty playbooks
    for operation in MOCK_SERVICE_DAG:
        if operation["name"].endswith("_start"):
            with (
                playbooks / (operation["name"].rstrip("_start") + "_restart.yml")
            ).open("w") as fd:
                pass
        with (playbooks / (operation["name"] + ".yml")).open("w") as fd:
            pass

    with (mock_defaults / "mock.yml").open("w") as fd:
        fd.write(yaml.dump({"key": "value", "another_key": "another_value"}))

    return Collections.from_collection_list([Collection.from_path(collection)])


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
