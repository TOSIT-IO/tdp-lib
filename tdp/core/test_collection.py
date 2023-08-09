# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest

from tdp.conftest import generate_collection
from tdp.core.collection import (
    DAG_DIRECTORY_NAME,
    Collection,
    MissingMandatoryDirectoryError,
    PathDoesNotExistsError,
    PathIsNotADirectoryError,
)


def test_collection_from_path_does_not_exist():
    with pytest.raises(PathDoesNotExistsError):
        Collection.from_path("foo")


def test_collection_from_path_is_not_a_directory(tmp_path: Path):
    empty_file = tmp_path / "foo"
    empty_file.touch()
    with pytest.raises(PathIsNotADirectoryError):
        Collection.from_path(empty_file)


def test_collection_from_path_missing_mandatory_directory(tmp_path: Path):
    with pytest.raises(MissingMandatoryDirectoryError):
        Collection.from_path(tmp_path)


def test_collection_from_path(tmp_path_factory: pytest.TempPathFactory):
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
    collection = Collection.from_path(collection_path)
    assert collection_path / DAG_DIRECTORY_NAME / "service.yml" in collection.dag_yamls
    assert "service_install" in collection.playbooks
    assert "service_config" in collection.playbooks
