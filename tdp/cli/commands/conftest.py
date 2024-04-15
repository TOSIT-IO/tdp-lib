# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest

from tdp.conftest import generate_collection_at_path


@pytest.fixture
def collection_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    collection_path = tmp_path_factory.mktemp("collection")
    dag_service_operations = {
        "service": [
            {"name": "service_install"},
            {"name": "service_config", "depends_on": ["service_install"]},
            {"name": "service_start", "depends_on": ["service_config"]},
            {"name": "service_init", "depends_on": ["service_start"]},
        ],
    }
    service_vars = {
        "service": {
            "service": {},
        },
    }
    generate_collection_at_path(collection_path, dag_service_operations, service_vars)
    return collection_path


@pytest.fixture
def database_dsn(tmp_path: Path) -> str:
    return "sqlite:///" + str(tmp_path / "sqlite.db")


@pytest.fixture
def vars(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("collection")
