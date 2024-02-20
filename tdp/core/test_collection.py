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
    get_collection_playbooks,
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


def test_init_collection_playbooks(tmp_path: Path):
    collection_path = tmp_path / "collection"
    playbook_directory = "playbooks"
    (playbook_directory_path := collection_path / playbook_directory).mkdir(
        parents=True, exist_ok=True
    )
    playbook_path_1 = playbook_directory_path / "playbook1.yml"
    playbook_path_2 = playbook_directory_path / "playbook2.yml"
    playbook_path_1.write_text(
        """---
- name: Play 1
  hosts: host1, host2
  tasks:
    - name: Task 1
      command: echo "Hello, World!"
"""
    )
    playbook_path_2.write_text(
        """---
- name: Play 2
  hosts: host3, host4
  tasks:
    - name: Task 2
      command: echo "Hello, GitHub Copilot!"
"""
    )
    playbooks = get_collection_playbooks(collection_path, playbook_directory)
    assert len(playbooks) == 2
    assert "playbook1" in playbooks
    assert "playbook2" in playbooks
    assert playbooks["playbook1"].path == playbook_path_1
    assert playbooks["playbook1"].collection_name == collection_path.name
    assert playbooks["playbook2"].path == playbook_path_2
    assert playbooks["playbook2"].collection_name == collection_path.name
