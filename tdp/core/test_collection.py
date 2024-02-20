# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import pytest

from tdp.conftest import generate_collection_at_path
from tdp.core.collection import (
    Collection,
    MissingMandatoryDirectoryError,
    PathDoesNotExistsError,
    PathIsNotADirectoryError,
    read_hosts_from_playbook,
)
from tdp.core.constants import DAG_DIRECTORY_NAME
from tdp.core.models.test_deployment_log import MockInventoryReader


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
    generate_collection_at_path(collection_path, dag_service_operations, service_vars)
    collection = Collection.from_path(collection_path)
    assert collection_path / DAG_DIRECTORY_NAME / "service.yml" in collection.dag_yamls
    assert "service_install" in collection.playbooks
    assert "service_config" in collection.playbooks


def test_read_hosts_from_playbook(tmp_path: Path):
    playbook_path = tmp_path / "playbook.yml"
    playbook_path.write_text(
        """---
- name: Play 1
  hosts: host1, host2
  tasks:
    - name: Task 1
      command: echo "Hello, World!"

"""
    )
    hosts = read_hosts_from_playbook(
        playbook_path, MockInventoryReader(["host1", "host2"])
    )
    assert hosts == {"host1", "host2"}
