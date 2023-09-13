# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.conftest import generate_collection_at_path
from tdp.core.cluster_status import ClusterStatus
from tdp.core.collection import Collection
from tdp.core.collections import Collections
from tdp.core.dag import Dag
from tdp.core.variables import ClusterVariables


@pytest.fixture(scope="session")
def mock_collections(tmp_path_factory: pytest.TempPathFactory) -> Collections:
    temp_collection_path = tmp_path_factory.mktemp("mock_collection")
    # Mock dag composed of 1 service ("serv") and 1 component ("comp")
    mock_dag = {
        "serv": [
            {"name": "serv_comp_install", "depends_on": []},
            {
                "name": "serv_comp_config",
                "depends_on": ["serv_comp_install"],
            },
            {
                "name": "serv_comp_start",
                "depends_on": ["serv_comp_config"],
            },
            {"name": "serv_comp_init", "depends_on": ["serv_comp_start"]},
            {
                "name": "serv_install",
                "noop": True,
                "depends_on": ["serv_comp_install"],
            },
            {
                "name": "serv_config",
                "noop": True,
                "depends_on": ["serv_install", "serv_comp_config"],
            },
            {
                "name": "serv_start",
                "noop": True,
                "depends_on": ["serv_config", "serv_comp_start"],
            },
            {
                "name": "serv_init",
                "noop": True,
                "depends_on": ["serv_start", "serv_comp_init"],
            },
        ],
    }
    # Mock vars for the "serv" service
    mock_vars = {
        "serv": {
            "serv": {
                "key": "value",
                "another_key": "another_value",
            },
        },
    }
    generate_collection_at_path(path=temp_collection_path, dag=mock_dag, vars=mock_vars)
    return Collections.from_collection_list(
        [Collection.from_path(temp_collection_path)]
    )


@pytest.fixture(scope="session")
def mock_dag(mock_collections: Collections) -> Dag:
    dag = Dag(mock_collections)
    # Validate the dag in case of declaration errors
    dag.validate()
    return dag


@pytest.fixture
def mock_cluster_variables(
    tmp_path_factory: pytest.TempPathFactory, mock_collections: Collections
) -> ClusterVariables:
    return ClusterVariables.initialize_cluster_variables(
        collections=mock_collections, tdp_vars=tmp_path_factory.mktemp("tdp_vars")
    )


@pytest.fixture
def mock_cluster_status():
    return ClusterStatus({})


# TODO: returns tuple[ClusterVariables, ClusterStatus]
# @pytest.fixture(scope="function")
# def reconfigurable_cluster_variables(
#     tmp_path_factory: pytest.TempPathFactory, mock_collections: Collections
# ) -> ClusterVariables:
#     tdp_vars = tmp_path_factory.mktemp("tdp_vars")
#     cluster_variables = ClusterVariables.initialize_cluster_variables(
#         mock_collections, tdp_vars
#     )

#     with cluster_variables["mock"].open_var_files(
#         "update serv configuration", ["mock.yml"]
#     ) as configuration:
#         configuration["mock.yml"].merge({"test": 1})
#     return cluster_variables
