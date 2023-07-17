# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import pytest

from tdp.conftest import generate_collection


@pytest.fixture
def collection_path(tmp_path_factory):
    collection_path = tmp_path_factory.mktemp("collection")
    dag_service_operations = {
        "service": [
            {"name": "service_install"},
            {"name": "service_config"},
        ],
    }
    service_vars = {
        "service": {
            "service": {},
        },
    }
    generate_collection(collection_path, dag_service_operations, service_vars)
    return collection_path


@pytest.fixture
def database_dsn():
    return "sqlite+pysqlite:///:memory:"


@pytest.fixture
def database_dsn_path(tmp_path):
    return "sqlite:///" + str(tmp_path / "sqlite.db")


@pytest.fixture
def vars(tmp_path_factory):
    return tmp_path_factory.mktemp("collection")
